"""
DASHBOARD DE MONITOREO EN TIEMPO REAL - TRANSPORTE PÚBLICO SF BAY AREA
Visualización interactiva de datos de la API 511.org
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import time

# ============================================================================
# CONFIGURACIÓN DE LA PÁGINA
# ============================================================================

st.set_page_config(
    page_title="🚌 Transit Monitor - SF Bay Area",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CONEXIÓN A BASE DE DATOS
# ============================================================================

def get_database_connection():
    """Crear conexión a PostgreSQL"""
    return psycopg2.connect(
        host='karenserver.postgres.database.azure.com',
        database='transit_streaming',
        user='admin_karen',
        port=5432,
        password = 'Tiendala60'
    )

# ============================================================================
# FUNCIONES PARA OBTENER DATOS
# ============================================================================

@st.cache_data(ttl=10)  # Cache por 10 segundos
def get_active_vehicles():
    """Obtener vehículos activos en los últimos 5 minutos"""
    conn = get_database_connection()
    try:
        query = """
            SELECT 
                vehicle_id,
                route_id,
                agency_id,
                latitude,
                longitude,
                speed,
                heading,
                timestamp
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            ORDER BY timestamp DESC
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

@st.cache_data(ttl=30)
def get_statistics():
    """Obtener estadísticas generales"""
    conn = get_database_connection()
    cursor = conn.cursor()
    
    try:
        stats = {}
        
        # Total de registros
        cursor.execute("SELECT COUNT(*) FROM vehicle_positions")
        stats['total_records'] = cursor.fetchone()[0]
        
        # Vehículos únicos activos
        cursor.execute("""
            SELECT COUNT(DISTINCT vehicle_id) 
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
        """)
        stats['active_vehicles'] = cursor.fetchone()[0]
        
        # Por agencia
        cursor.execute("""
            SELECT agency_id, COUNT(DISTINCT vehicle_id) as count
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            GROUP BY agency_id
        """)
        stats['by_agency'] = dict(cursor.fetchall())
        
        # Velocidad promedio
        cursor.execute("""
            SELECT AVG(speed) 
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            AND speed IS NOT NULL
        """)
        avg_speed = cursor.fetchone()[0]
        stats['avg_speed'] = round(avg_speed, 2) if avg_speed else 0
        
        # Última actualización
        cursor.execute("""
            SELECT MAX(timestamp) 
            FROM vehicle_positions
        """)
        stats['last_update'] = cursor.fetchone()[0]
        
        return stats
    finally:
        cursor.close()
        conn.close()

@st.cache_data(ttl=30)
def get_route_statistics():
    """Obtener estadísticas por ruta"""
    conn = get_database_connection()
    try:
        query = """
            SELECT 
                route_id,
                agency_id,
                COUNT(DISTINCT vehicle_id) as vehicles,
                AVG(speed) as avg_speed,
                COUNT(*) as total_records
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '1 hour'
            AND route_id IS NOT NULL
            GROUP BY route_id, agency_id
            ORDER BY vehicles DESC
            LIMIT 15
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

@st.cache_data(ttl=30)
def get_hourly_activity():
    """Obtener actividad por hora"""
    conn = get_database_connection()
    try:
        query = """
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(DISTINCT vehicle_id) as vehicles,
                COUNT(*) as records
            FROM vehicle_positions
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

# ============================================================================
# INTERFAZ PRINCIPAL
# ============================================================================

# Título
st.title("🚌 Monitor de Transporte Público en Tiempo Real")
st.markdown("**Área de la Bahía de San Francisco** | Fuente: 511.org API")

# Barra lateral
st.sidebar.title("⚙️ Configuración")
st.sidebar.markdown("---")

# Auto-refresh
auto_refresh = st.sidebar.checkbox("🔄 Auto-actualizar", value=True)
if auto_refresh:
    refresh_interval = st.sidebar.slider("Intervalo (segundos)", 5, 60, 10)
    st.sidebar.info(f"Actualizando cada {refresh_interval}s")

# Filtros
st.sidebar.markdown("### 🔍 Filtros")
selected_agencies = st.sidebar.multiselect(
    "Agencias",
    ["SF", "AC", "CT"],
    default=["SF", "AC", "CT"]
)

# ============================================================================
# OBTENER DATOS
# ============================================================================

try:
    stats = get_statistics()
    vehicles_df = get_active_vehicles()
    
    # Filtrar por agencias seleccionadas
    if selected_agencies:
        vehicles_df = vehicles_df[vehicles_df['agency_id'].isin(selected_agencies)]
    
    # ============================================================================
    # SECCIÓN 1: KPIs PRINCIPALES
    # ============================================================================
    
    st.markdown("---")
    st.subheader("📊 Indicadores en Tiempo Real")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🚌 Vehículos Activos",
            value=f"{len(vehicles_df):,}",
            delta=f"Total: {stats['active_vehicles']:,}"
        )
    
    with col2:
        st.metric(
            label="📍 Registros Totales",
            value=f"{stats['total_records']:,}",
            delta=None
        )
    
    with col3:
        st.metric(
            label="⚡ Velocidad Promedio",
            value=f"{stats['avg_speed']} km/h",
            delta=None
        )
    
    with col4:
        if stats['last_update']:
            time_diff = datetime.now() - stats['last_update'].replace(tzinfo=None)
            seconds_ago = int(time_diff.total_seconds())
            st.metric(
                label="🕐 Última Actualización",
                value=f"Hace {seconds_ago}s",
                delta=stats['last_update'].strftime("%H:%M:%S")
            )
        else:
            st.metric(label="🕐 Última Actualización", value="N/A")
    
    # ============================================================================
    # SECCIÓN 2: VEHÍCULOS POR AGENCIA
    # ============================================================================
    
    st.markdown("---")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("🏢 Vehículos por Agencia")
        
        agency_names = {
            'SF': 'SF Muni',
            'AC': 'AC Transit',
            'CT': 'Caltrain'
        }
        
        for agency_id in ['SF', 'AC', 'CT']:
            count = stats['by_agency'].get(agency_id, 0)
            st.metric(
                label=agency_names[agency_id],
                value=f"{count} vehículos"
            )
    
    with col2:
        st.subheader("📈 Distribución por Agencia")
        if stats['by_agency']:
            agency_data = pd.DataFrame([
                {'Agencia': agency_names[k], 'Vehículos': v} 
                for k, v in stats['by_agency'].items()
            ])
            
            fig = px.pie(
                agency_data,
                values='Vehículos',
                names='Agencia',
                color='Agencia',
                color_discrete_map={
                    'SF Muni': '#E31837',
                    'AC Transit': '#00A94F',
                    'Caltrain': '#D2232A'
                },
                hole=0.4
            )
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)
    
    # ============================================================================
    # SECCIÓN 3: MAPA DE VEHÍCULOS
    # ============================================================================
    
    st.markdown("---")
    st.subheader("🗺️ Posiciones de Vehículos en Tiempo Real")
    
    if not vehicles_df.empty:
        # Preparar datos para el mapa
        map_df = vehicles_df.copy()
        
        # Asignar colores por agencia
        color_map = {'SF': 'red', 'AC': 'green', 'CT': 'blue'}
        map_df['color'] = map_df['agency_id'].map(color_map)
        
        # Crear texto para hover
        map_df['hover_text'] = (
            "Vehículo: " + map_df['vehicle_id'].astype(str) + "<br>" +
            "Ruta: " + map_df['route_id'].fillna('N/A').astype(str) + "<br>" +
            "Agencia: " + map_df['agency_id'].astype(str) + "<br>" +
            "Velocidad: " + map_df['speed'].fillna(0).round(1).astype(str) + " km/h<br>" +
            "Rumbo: " + map_df['heading'].fillna(0).astype(str) + "°"
        )
        
        # Crear mapa con Plotly
        fig = go.Figure()
        
        for agency in map_df['agency_id'].unique():
            agency_data = map_df[map_df['agency_id'] == agency]
            
            fig.add_trace(go.Scattermapbox(
                lat=agency_data['latitude'],
                lon=agency_data['longitude'],
                mode='markers',
                marker=dict(
                    size=8,
                    color=agency_data['color'].iloc[0],
                    opacity=0.7
                ),
                text=agency_data['hover_text'],
                hoverinfo='text',
                name=agency
            ))
        
        # Configurar mapa
        fig.update_layout(
            mapbox=dict(
                style="open-street-map",
                center=dict(
                    lat=map_df['latitude'].mean(),
                    lon=map_df['longitude'].mean()
                ),
                zoom=10
            ),
            height=600,
            margin=dict(l=0, r=0, t=0, b=0),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255,255,255,0.8)"
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Estadísticas del mapa
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"🔴 SF Muni: {len(map_df[map_df['agency_id']=='SF'])} vehículos")
        with col2:
            st.success(f"🟢 AC Transit: {len(map_df[map_df['agency_id']=='AC'])} vehículos")
        with col3:
            st.error(f"🔵 Caltrain: {len(map_df[map_df['agency_id']=='CT'])} vehículos")
    else:
        st.warning("⚠️ No hay vehículos activos en este momento")
    
    # ============================================================================
    # SECCIÓN 4: RUTAS MÁS ACTIVAS
    # ============================================================================
    
    st.markdown("---")
    st.subheader("🚏 Top 15 Rutas Más Activas (Última Hora)")
    
    route_stats = get_route_statistics()
    
    if not route_stats.empty:
        # Gráfico de barras
        fig = px.bar(
            route_stats,
            x='route_id',
            y='vehicles',
            color='agency_id',
            color_discrete_map={'SF': '#E31837', 'AC': '#00A94F', 'CT': '#D2232A'},
            labels={'vehicles': 'Vehículos', 'route_id': 'Ruta'},
            title="Vehículos Activos por Ruta"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla detallada
        st.dataframe(
            route_stats.rename(columns={
                'route_id': 'Ruta',
                'agency_id': 'Agencia',
                'vehicles': 'Vehículos',
                'avg_speed': 'Velocidad Prom. (km/h)',
                'total_records': 'Registros'
            }).style.format({
                'Velocidad Prom. (km/h)': '{:.1f}',
                'Vehículos': '{:.0f}',
                'Registros': '{:.0f}'
            }),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("📊 No hay suficientes datos para mostrar estadísticas de rutas")
    
    # ============================================================================
    # SECCIÓN 5: ACTIVIDAD HISTÓRICA
    # ============================================================================
    
    st.markdown("---")
    st.subheader("📅 Actividad en las Últimas 24 Horas")
    
    hourly_data = get_hourly_activity()
    
    if not hourly_data.empty:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=hourly_data['hour'],
            y=hourly_data['vehicles'],
            mode='lines+markers',
            name='Vehículos Únicos',
            line=dict(color='#E31837', width=3),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            xaxis_title="Hora",
            yaxis_title="Vehículos Activos",
            height=350,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("📈 Acumulando datos históricos...")
    
    # ============================================================================
    # FOOTER
    # ============================================================================
    
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray; padding: 20px;'>
            <p>🚌 Sistema de Monitoreo de Transporte Público en Tiempo Real</p>
            <p>Datos proporcionados por <a href='https://511.org' target='_blank'>511.org</a></p>
            <p>Última actualización: {}</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

except Exception as e:
    st.error(f"❌ Error al conectar con la base de datos: {e}")
    st.info("💡 Asegúrate de que:")
    st.markdown("""
    1. PostgreSQL esté corriendo
    2. La base de datos 'transit_streaming' exista
    3. El script de ingesta esté activo
    4. Las credenciales sean correctas
    """)
