"""
SISTEMA DE INGESTA DE DATOS DE TRANSPORTE PÚBLICO - 511.org API
Script que obtiene datos en tiempo real de la API de 511.org y los inserta en PostgreSQL
"""

import requests
import psycopg2
from psycopg2.extras import execute_batch
import time
from datetime import datetime
import json
import gzip
import io

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Configuración de la base de datos PostgreSQL
DB_CONFIG = {
    'host': 'karenserver.postgres.database.azure.com',
    'database': 'transit_streaming',
    'user': 'admin_karen',
    'password': 'Tiendala60',
    'port': 5432
}

# Configuración de la API 511.org
API_CONFIG = {
    'api_key': '8ca98f61-be18-426e-942f-04be6f49ff66',
    'base_url': 'http://api.511.org',
    'agencies': ['SF', 'AC', 'CT'],  # SF Muni, AC Transit, Caltrain
    'rate_limit_delay': 2  # Segundos entre requests (max 60 req/hora)
}

# ============================================================================
# SETUP DE BASE DE DATOS
# ============================================================================

def setup_database():
    """Crear tablas si no existen"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Tabla 1: Posiciones de vehículos en tiempo real
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vehicle_positions (
            id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(100) NOT NULL,
            route_id VARCHAR(50),
            trip_id VARCHAR(100),
            agency_id VARCHAR(50) NOT NULL,
            latitude DECIMAL(10, 7) NOT NULL,
            longitude DECIMAL(10, 7) NOT NULL,
            speed DECIMAL(5, 2),
            heading INT,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_vehicle_timestamp UNIQUE (vehicle_id, timestamp)
        );
    ''')
    
    # Índices para optimizar queries
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_vehicle_timestamp 
        ON vehicle_positions(timestamp DESC);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_vehicle_agency 
        ON vehicle_positions(agency_id);
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_vehicle_route 
        ON vehicle_positions(route_id);
    ''')
    
    # Tabla 2: Información de rutas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS routes (
            route_id VARCHAR(50) PRIMARY KEY,
            route_name VARCHAR(200),
            agency_id VARCHAR(50) NOT NULL,
            route_type VARCHAR(50),
            total_vehicles INT DEFAULT 0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    
    # Tabla 3: Alertas y anomalías detectadas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transit_alerts (
            id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(100),
            route_id VARCHAR(50),
            agency_id VARCHAR(50),
            alert_type VARCHAR(100) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            description TEXT,
            latitude DECIMAL(10, 7),
            longitude DECIMAL(10, 7),
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_alerts_time 
        ON transit_alerts(detected_at DESC);
    ''')
    
    # Tabla 4: Estadísticas agregadas (para dashboard más rápido)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS route_statistics (
            id SERIAL PRIMARY KEY,
            route_id VARCHAR(50) NOT NULL,
            agency_id VARCHAR(50) NOT NULL,
            active_vehicles INT DEFAULT 0,
            avg_speed DECIMAL(5, 2),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✅ Base de datos configurada correctamente")
    print("   • vehicle_positions")
    print("   • routes")
    print("   • transit_alerts")
    print("   • route_statistics")

# ============================================================================
# FUNCIONES PARA LA API 511.org
# ============================================================================

class Transit511API:
    def __init__(self, api_config):
        self.api_key = api_config['api_key']
        self.base_url = api_config['base_url']
        self.agencies = api_config['agencies']
        self.delay = api_config['rate_limit_delay']
        
        if self.api_key == 'YOUR_API_KEY_HERE':
            print("\n⚠️  ERROR: Necesitas un API key de 511.org")
            print("   Solicítalo en: https://511.org/open-data/token")
            raise ValueError("API key no configurada")
    
    def fetch_vehicle_positions(self, agency):
        """Obtener posiciones de vehículos en tiempo real"""
        endpoint = f"{self.base_url}/transit/VehicleMonitoring"
        
        params = {
            'api_key': self.api_key,
            'agency': agency,
            'format': 'json'
        }
        
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            
            if response.status_code == 200:
                # La API devuelve JSON con UTF-8 BOM, necesitamos limpiarlo
                try:
                    # Decodificar quitando el BOM
                    content = response.content.decode('utf-8-sig')
                    data = json.loads(content)
                except Exception as e:
                    print(f"   ⚠️  Error parseando respuesta: {e}")
                    return []
                
                return self.parse_vehicle_data(data, agency)
            else:
                print(f"   ⚠️  Error HTTP {response.status_code} para agencia {agency}")
                return []
        
        except requests.exceptions.Timeout:
            print(f"   ⏰ Timeout al consultar agencia {agency}")
            return []
        
        except Exception as e:
            print(f"   ⚠️  Error: {e}")
            return []
    
    def parse_vehicle_data(self, data, agency):
        """Parsear datos JSON de vehículos"""
        vehicles = []
        
        try:
            # Estructura de respuesta de 511 API
            if 'Siri' in data and 'ServiceDelivery' in data['Siri']:
                delivery = data['Siri']['ServiceDelivery']
                
                if 'VehicleMonitoringDelivery' in delivery:
                    vm_delivery = delivery['VehicleMonitoringDelivery']
                    
                    # VehicleMonitoringDelivery es un dict que contiene VehicleActivity
                    if isinstance(vm_delivery, dict):
                        # La estructura correcta es VehicleActivity (array de actividades)
                        vehicle_activities = vm_delivery.get('VehicleActivity', [])
                        
                        # Si no es una lista, convertirla a lista
                        if not isinstance(vehicle_activities, list):
                            vehicle_activities = [vehicle_activities] if vehicle_activities else []
                        
                        for activity in vehicle_activities:
                            try:
                                # Dentro de cada VehicleActivity está MonitoredVehicleJourney
                                if 'MonitoredVehicleJourney' in activity:
                                    journey = activity['MonitoredVehicleJourney']
                                    vehicle = self.extract_vehicle_info(journey, agency)
                                    if vehicle:
                                        vehicles.append(vehicle)
                            except Exception:
                                # Saltar vehículos con datos inválidos silenciosamente
                                continue
        
        except Exception as e:
            print(f"   ⚠️  Error parseando datos: {e}")
        
        return vehicles
    
    def extract_vehicle_info(self, journey, agency):
        """Extraer información relevante de un vehículo"""
        try:
            # Información del vehículo
            vehicle_ref = journey.get('VehicleRef', 'unknown')
            
            # Información de ubicación
            location = journey.get('VehicleLocation', {})
            latitude = float(location.get('Latitude', 0))
            longitude = float(location.get('Longitude', 0))
            
            # Si no hay coordenadas válidas, saltar
            if latitude == 0 or longitude == 0:
                return None
            
            # Información de ruta y viaje
            line_ref = journey.get('LineRef', None)
            framed_vehicle_journey = journey.get('FramedVehicleJourneyRef', {})
            trip_id = framed_vehicle_journey.get('DatedVehicleJourneyRef', None)
            
            # Velocidad y dirección
            speed = journey.get('Speed', None)
            heading = journey.get('Bearing', None)
            
            # Timestamp
            recorded_time = journey.get('RecordedAtTime', None)
            if recorded_time:
                # Parsear ISO timestamp
                timestamp = datetime.fromisoformat(recorded_time.replace('Z', '+00:00'))
            else:
                timestamp = datetime.now()
            
            vehicle_data = {
                'vehicle_id': vehicle_ref,
                'route_id': line_ref,
                'trip_id': trip_id,
                'agency_id': agency,
                'latitude': latitude,
                'longitude': longitude,
                'speed': float(speed) if speed else None,
                'heading': int(float(heading)) if heading else None,
                'timestamp': timestamp
            }
            
            return vehicle_data
        
        except Exception:
            return None

# ============================================================================
# CLASE PARA MANEJAR BASE DE DATOS
# ============================================================================

class TransitDatabase:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = psycopg2.connect(**db_config)
    
    def insert_vehicle_positions(self, vehicles):
        """Insertar posiciones de vehículos en batch"""
        if not vehicles:
            return 0
        
        cursor = self.conn.cursor()
        
        # Contar registros antes de insertar
        cursor.execute("SELECT COUNT(*) FROM vehicle_positions")
        count_before = cursor.fetchone()[0]
        
        insert_query = """
            INSERT INTO vehicle_positions 
            (vehicle_id, route_id, trip_id, agency_id, latitude, longitude, 
             speed, heading, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vehicle_id, timestamp) DO NOTHING
        """
        
        data = [
            (v['vehicle_id'], v['route_id'], v['trip_id'], v['agency_id'],
             v['latitude'], v['longitude'], v['speed'], v['heading'], v['timestamp'])
            for v in vehicles
        ]
        
        execute_batch(cursor, insert_query, data)
        
        # Contar registros después de insertar
        cursor.execute("SELECT COUNT(*) FROM vehicle_positions")
        count_after = cursor.fetchone()[0]
        
        inserted = count_after - count_before
        print("se insertaron", inserted, "registros nuevos en vehicle_positions.")
        
        self.conn.commit()
        cursor.close()
        
        return inserted
    
    def update_route_info(self, vehicles):
        """Actualizar información de rutas"""
        cursor = self.conn.cursor()
        
        # Agrupar vehículos por ruta
        routes = {}
        for v in vehicles:
            key = (v['route_id'], v['agency_id'])
            if key not in routes:
                routes[key] = []
            routes[key].append(v)
        
        # Actualizar cada ruta
        for (route_id, agency_id), route_vehicles in routes.items():
            if not route_id:
                continue
            
            cursor.execute("""
                INSERT INTO routes (route_id, agency_id, total_vehicles, last_update)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (route_id) 
                DO UPDATE SET 
                    total_vehicles = EXCLUDED.total_vehicles,
                    last_update = CURRENT_TIMESTAMP
            """, (route_id, agency_id, len(route_vehicles)))

            print(f"Ruta {route_id} actualizada con {len(route_vehicles)} vehículos.")
        
        self.conn.commit()
        cursor.close()
    
    def detect_anomalies(self, vehicles):
        """Detectar anomalías simples en los datos"""
        anomalies = []
        
        for v in vehicles:
            # Anomalía 1: Coordenadas fuera del área de SF Bay
            if not (37.0 <= v['latitude'] <= 38.5 and -123.0 <= v['longitude'] <= -121.5):
                anomalies.append({
                    'vehicle_id': v['vehicle_id'],
                    'route_id': v['route_id'],
                    'agency_id': v['agency_id'],
                    'alert_type': 'location_anomaly',
                    'severity': 'medium',
                    'description': f"Vehículo fuera del área esperada: {v['latitude']}, {v['longitude']}",
                    'latitude': v['latitude'],
                    'longitude': v['longitude']
                })
            
            # Anomalía 2: Rumbo inválido
            if v['heading'] and (v['heading'] < 0 or v['heading'] > 360):
                anomalies.append({
                    'vehicle_id': v['vehicle_id'],
                    'route_id': v['route_id'],
                    'agency_id': v['agency_id'],
                    'alert_type': 'invalid_heading',
                    'severity': 'low',
                    'description': f"Rumbo inválido: {v['heading']}°",
                    'latitude': v['latitude'],
                    'longitude': v['longitude']
                })
        
        if anomalies:
            self.insert_alerts(anomalies)
        
        return len(anomalies)
    
    def insert_alerts(self, alerts):
        """Insertar alertas en la base de datos"""
        if not alerts:
            return
        
        cursor = self.conn.cursor()
        
        insert_query = """
            INSERT INTO transit_alerts 
            (vehicle_id, route_id, agency_id, alert_type, severity, 
             description, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        data = [
            (a['vehicle_id'], a['route_id'], a['agency_id'], a['alert_type'],
             a['severity'], a['description'], a['latitude'], a['longitude'])
            for a in alerts
        ]
        
        execute_batch(cursor, insert_query, data)
        self.conn.commit()
        cursor.close()
    
    def get_statistics(self):
        """Obtener estadísticas generales"""
        cursor = self.conn.cursor()
        
        # Total de registros
        cursor.execute("SELECT COUNT(*) FROM vehicle_positions")
        total_positions = cursor.fetchone()[0]
        
        # Vehículos activos (última hora)
        cursor.execute("""
            SELECT COUNT(DISTINCT vehicle_id) 
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """)
        active_vehicles = cursor.fetchone()[0]
        
        # Alertas recientes
        cursor.execute("""
            SELECT COUNT(*) 
            FROM transit_alerts
            WHERE detected_at > NOW() - INTERVAL '1 hour'
        """)
        recent_alerts = cursor.fetchone()[0]
        
        cursor.close()
        
        return {
            'total_positions': total_positions,
            'active_vehicles': active_vehicles,
            'recent_alerts': recent_alerts
        }

# ============================================================================
# PROCESO PRINCIPAL DE STREAMING
# ============================================================================

class TransitStreamer:
    def __init__(self, api_config, db_config):
        self.api = Transit511API(api_config)
        self.db = TransitDatabase(db_config)
        self.iteration = 0
    
    def run_single_fetch(self):
        """Ejecutar una iteración de obtención de datos"""
        self.iteration += 1
        
        print(f"\n{'='*70}")
        print(f"🔄 ITERACIÓN {self.iteration} | {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*70}")
        
        all_vehicles = []
        
        # Obtener datos de cada agencia
        for agency in self.api.agencies:
            print(f"\n📡 Consultando agencia: {agency}")
            vehicles = self.api.fetch_vehicle_positions(agency)
            
            if vehicles:
                print(f"   ✅ {len(vehicles)} vehículos obtenidos")
                all_vehicles.extend(vehicles)
            else:
                print(f"   ⚠️  No se obtuvieron datos")
            
            # Rate limiting
            time.sleep(self.api.delay)
        
        # Insertar en base de datos
        if all_vehicles:
            print(f"\n💾 Insertando {len(all_vehicles)} posiciones en BD...")
            inserted = self.db.insert_vehicle_positions(all_vehicles)
            print(f"   ✅ {inserted} registros insertados")
            
            # Actualizar rutas
            self.db.update_route_info(all_vehicles)
            
            # Detectar anomalías
            anomalies = self.db.detect_anomalies(all_vehicles)
            if anomalies > 0:
                print(f"   🚨 {anomalies} anomalías detectadas")
        else:
            print("\n⚠️  No se obtuvieron datos de ninguna agencia")
        
        # Mostrar estadísticas cada 5 iteraciones
        if self.iteration % 5 == 0:
            stats = self.db.get_statistics()
            print(f"\n📊 ESTADÍSTICAS:")
            print(f"   • Total de posiciones: {stats['total_positions']:,}")
            print(f"   • Vehículos activos (1h): {stats['active_vehicles']}")
            print(f"   • Alertas recientes (1h): {stats['recent_alerts']}")
    
    def run_streaming(self, interval_seconds=60):
        """Ejecutar streaming continuo"""
        print("\n" + "="*70)
        print("🚀 INICIANDO SISTEMA DE STREAMING DE TRANSPORTE PÚBLICO")
        print("="*70)
        print(f"📡 Agencias: {', '.join(self.api.agencies)}")
        print(f"⏱️  Intervalo: {interval_seconds} segundos")
        print(f"💾 Base de datos: {self.db.db_config['database']}")
        print("🛑 Presiona Ctrl+C para detener\n")
        
        try:
            while True:
                self.run_single_fetch()
                
                print(f"\n⏳ Esperando {interval_seconds} segundos...")
                time.sleep(interval_seconds)
        
        except KeyboardInterrupt:
            print("\n\n" + "="*70)
            print("⏹️  SISTEMA DETENIDO")
            print("="*70)
            
            # Estadísticas finales
            stats = self.db.get_statistics()
            print(f"\n📊 ESTADÍSTICAS FINALES:")
            print(f"   • Iteraciones completadas: {self.iteration}")
            print(f"   • Total de posiciones: {stats['total_positions']:,}")
            print(f"   • Vehículos activos: {stats['active_vehicles']}")
            print(f"   • Alertas generadas: {stats['recent_alerts']}")
            
            print("\n✅ Conexión cerrada correctamente")

# ============================================================================
# EJECUTAR
# ============================================================================

if __name__ == "__main__":
    print("\n🚌 SISTEMA DE MONITOREO DE TRANSPORTE PÚBLICO EN TIEMPO REAL")
    print("   Fuente: 511.org Transit API (San Francisco Bay Area)\n")
    
    # Setup de base de datos
    setup_database()
    
    print("\n")
    
    # Iniciar streaming
    streamer = TransitStreamer(API_CONFIG, DB_CONFIG)
    streamer.run_streaming(interval_seconds=60)
