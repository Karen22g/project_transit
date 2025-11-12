"""
DASHBOARD DE MONITOREO EN TIEMPO REAL - TRANSPORTE PÚBLICO SF BAY AREA
Visualización interactiva de datos de la API 511.org
"""

import altair as alt
import folium
from scipy import stats
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
#import psycopg2
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import time
import pg8000
from streamlit_folium import st_folium

# ============================================================================
# CONFIGURACIÓN DE LA PÁGINA
# ============================================================================

st.set_page_config(
    page_title="🚌 Tránsito actual en San Francisco",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CONEXIÓN A BASE DE DATOS
# ============================================================================

def get_database_connection():
    """Crear conexión a PostgreSQL"""
    return pg8000.connect(
    host="karenserver.postgres.database.azure.com",
    database="transit_streaming",
    user="admin_karen",
    password="Tiendala60",
    port=5432)

# psycopg2.connect(
#         host='karenserver.postgres.database.azure.com',
#         database='transit_streaming',
#         user='admin_karen',
#         port=5432,
#         password = 'Tiendala60'
#     )

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
                timestamp,
                created_at,
                trip_id
            FROM vehicle_positions
            WHERE created_at > NOW() - INTERVAL '10 minutes'
            ORDER BY created_at DESC
        """
        df = pd.read_sql(query, conn)
        print(df)
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
            WHERE created_at > NOW() - INTERVAL '5 minutes'
        """)
        stats['active_vehicles'] = cursor.fetchone()[0]
        
        # Por agencia
        cursor.execute("""
            SELECT agency_id, COUNT(DISTINCT vehicle_id) as count
            FROM vehicle_positions
            WHERE created_at > NOW() - INTERVAL '5 minutes'
            GROUP BY agency_id
        """)
        stats['by_agency'] = dict(cursor.fetchall())
        
        # Velocidad promedio
        cursor.execute("""
            SELECT AVG(speed) 
            FROM vehicle_positions
            WHERE created_at > NOW() - INTERVAL '5 minutes'
            AND speed IS NOT NULL
        """)

        avg_speed = cursor.fetchone()[0]
        print("Average speed (m/s):", avg_speed)

        if avg_speed:
            avg_speed_mph = float(avg_speed) * 2.23694  # conversión de m/s a mi/h
            stats['avg_speed'] = round(avg_speed_mph, 2)
        else:
            stats['avg_speed'] = 0
        
        # Última actualización
        cursor.execute("""SELECT MAX(timestamp) FROM vehicle_positions""")
        stats['last_update'] = cursor.fetchone()[0]  # mantener datetime real
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
            WHERE created_at > NOW() - INTERVAL '1 hour'
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
            WHERE created_at > NOW() - INTERVAL '24 hours'
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
st.title("🚌 Tránsito actual en San Francisco")
st.markdown("**Bahía de San Francisco** | Fuente: 511.org API")

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
    
    df = vehicles_df.copy()
    df['created_at'] = pd.to_datetime(df['created_at'])
    
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
            value=f"{stats['avg_speed']} mi/h",
            delta=None
        )
    
    with col4:
        last_update = stats.get('last_update')
        print("Última actualización cruda:", last_update)

        if last_update:
            # Asegurar que sea tipo datetime sin zona horaria
            last_update = last_update.replace(tzinfo=None)
            time_diff = datetime.now() - last_update

            # Formatear fecha y hora legibles
            formatted_time = last_update.strftime("%Y-%m-%d %H:%M:%S")

            st.metric(
                label="🕐 Última Actualización",
                value=f"{formatted_time}",
                delta=None  # sin delta
            )

        else:
            st.metric(label="🕐 Última Actualización", value="Sin datos")

    # ============================================================================
    # SECCIÓN 2: Agencias
    # ============================================================================

    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["week"] = df["timestamp"].dt.isocalendar().week
    df["month"] = df["timestamp"].dt.month
    df["is_active"] = df["created_at"] > (pd.Timestamp.now() - pd.Timedelta(minutes=5))
    st.markdown("---")
    st.subheader("🏢 Resumen por Agencia")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        agency_summary = (
            df.groupby("agency_id")
            .agg(
                total_vehículos=("vehicle_id", "nunique"),
                activos=("is_active", "sum"),
                rutas=("route_id", "nunique")
            )
            .reset_index()
        )
        agency_summary["% uso"] = (
            100 * agency_summary["activos"] / agency_summary["total_vehículos"]
        ).round(1)
        # Preparar datos para pie chart
        pie_data = agency_summary.copy()
        pie_data = pie_data[["agency_id", "% uso"]].rename(columns={"agency_id": "Agencia", "% uso": "Porcentaje"})

        # Crear diagrama de pastel
        pie_chart = alt.Chart(pie_data).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="Porcentaje", type="quantitative"),
            color=alt.Color(field="Agencia", type="nominal"),
            tooltip=["Agencia", "Porcentaje"]
        ).properties(
            width=400,
            height=400,
            title="Porcentaje de vehículos activos por agencia"
        )

        st.altair_chart(pie_chart, use_container_width=True)
        
    with col2:
        fig_routes = px.bar(
            agency_summary,
            x="agency_id",
            y="rutas",
            color="rutas",
            title="Número de rutas cubiertas por cada agencia",
        )
        st.plotly_chart(fig_routes, use_container_width=True)
    
    agency_summary = agency_summary.rename(columns={
        "agency_id": "Agencia",
        "total_vehículos": "Total de Vehículos",
        "activos": "Vehículos Activos",
        "rutas": "Rutas"
    })

    st.dataframe(agency_summary, use_container_width=True)

    # 4️⃣ Mapa: ubicación actual de vehículos por agencia
    st.subheader("🗺️ Ubicación actual de los vehículos activos")

    df_active = df[df["is_active"] & df["latitude"].notna() & df["longitude"].notna()]

    if not df_active.empty:
        m = folium.Map(
            location=[df_active["latitude"].mean(), df_active["longitude"].mean()],
            zoom_start=10,
            tiles="cartodbpositron"
        )

        colors = ["red", "blue", "green", "purple", "orange", "darkred", "cadetblue"]
        color_map = {a: colors[i % len(colors)] for i, a in enumerate(df_active["agency_id"].unique())}

        for _, row in df_active.iterrows():
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=4,
                color=color_map.get(row["agency_id"], "gray"),
                fill=True,
                fill_opacity=0.8,
                popup=(
                    f"🚍 Vehículo: {row['vehicle_id']}<br>"
                    f"Agencia: {row['agency_id']}<br>"
                    f"Ruta: {row['route_id']}<br>"
                    f"Hora: {row['timestamp'].strftime('%H:%M:%S')}"
                ),
            ).add_to(m)

        st_folium(m, height=500, width=1500)
    else:
        st.info("No hay vehículos activos con coordenadas disponibles para mostrar en el mapa.")

    # ============================================================================
    # SECCIÓN 3: Vehículos
    # ============================================================================

    st.markdown("---")
    st.subheader("📊 Resumen general por vehículo")
    
    col1, col2 = st.columns([1, 2], gap = 'medium', vertical_alignment='bottom')

    with col1:
        # Selector de periodo
        time_scale = st.selectbox("Selecciona el período:", ["Día", "Semana", "Mes"])

        # Agrupar según el periodo
        if time_scale == "Día":
            active_vehicle = (
                df.groupby(["vehicle_id", "date"])['trip_id']
                .nunique()
                .reset_index(name="viajes")
                .sort_values("viajes", ascending=False)
            )
            period_label = "hoy"
        elif time_scale == "Semana":
            active_vehicle = (
                df.groupby(["vehicle_id", "week"])['trip_id']
                .nunique()
                .reset_index(name="viajes")
                .sort_values("viajes", ascending=False)
            )
            period_label = "esta semana"
        else:
            active_vehicle = (
                df.groupby(["vehicle_id", "month"])['trip_id']
                .nunique()
                .reset_index(name="viajes")
                .sort_values("viajes", ascending=False)
            )
            period_label = "este mes"

        # Obtener top 5 vehículos
        top5 = active_vehicle.groupby("vehicle_id")["viajes"].sum().sort_values(ascending=False).head(5).reset_index()
        top_vehicle = top5.iloc[0]["vehicle_id"]
        df_speed_avg = df.groupby(["vehicle_id", "agency_id"])["speed"].mean().reset_index()
        df_speed_avg["speed"] = df_speed_avg["speed"].fillna(0)  # rellenar NaN con 0

        # Mostrar métrica del top_vehicle
        df_top = df_speed_avg[df_speed_avg["vehicle_id"] == top_vehicle]
        avg_speed_top = df_top["speed"].values[0] if not df_top.empty else 0

        # Mostrar métrica
        st.metric(f"Velocidad promedio del vehículo más activo: #{top_vehicle} (mi/h)", round(avg_speed_top, 2))

        # Diagrama de barras del top 5
        bar_chart = alt.Chart(top5).mark_bar(color="#1f77b4").encode(
            x=alt.X("vehicle_id:N", title="ID del Vehículo"),
            y=alt.Y("viajes:Q", title="Número de viajes"),
            tooltip=["vehicle_id", "viajes"]
        ).properties(
            width=600,
            height=400,
            title=f"Top 5 vehículos más activos ({period_label})"
        )

        st.altair_chart(bar_chart, use_container_width=True)
    
    with col2:
        # Diagrama de dispersión
        scatter = alt.Chart(df_speed_avg).mark_circle(size=100).encode(
            x=alt.X("vehicle_id:N", title="ID del Vehículo"),
            y=alt.Y("speed:Q", title="Velocidad Promedio (mi/h)"),
            color=alt.Color("agency_id:N", title="Agencia"),
            tooltip=[
                alt.Tooltip("vehicle_id:N", title="Vehículo"),
                alt.Tooltip("agency_id:N", title="Agencia"),
                alt.Tooltip("speed:Q", title="Velocidad Promedio (mi/h)", format=".2f")
            ]
        ).properties(
            width=700,
            height=400,
            title="Velocidad promedio por vehículo"
        ).interactive()  # permite hacer zoom y pan

        st.altair_chart(scatter, use_container_width=True)
    
    df_max_speed = df.groupby(["vehicle_id", "agency_id", "trip_id", "route_id"])["speed"].max().reset_index()

    # Calcular velocidad promedio por vehículo
    df_avg_speed = df.groupby(["vehicle_id", "agency_id"])["speed"].mean().reset_index()
    df_avg_speed["speed"] = df_avg_speed["speed"].fillna(0)

    # Combinar velocidad promedio con la info de max speed
    df_top_speed = pd.merge(
        df_avg_speed,
        df_max_speed,
        on=["vehicle_id", "agency_id"],
        suffixes=("_avg", "_max")
    )

    # Tomar top 5 por velocidad promedio
    top5_speed = df_top_speed.sort_values("speed_avg", ascending=False).head(5)

    st.subheader("🏎️ Top 5 vehículos por velocidad promedio")
    st.write(
        "Estos son los vehículos con mayor velocidad promedio, mostrando también la agencia, trip y ruta donde alcanzaron su velocidad máxima:"
    )

    # Mostrar tabla
    st.dataframe(top5_speed[["vehicle_id", "agency_id", "speed_avg", "trip_id", "route_id", "speed_max"]].rename(
        columns={
            "vehicle_id": "Vehículo",
            "agency_id": "Agencia",
            "speed_avg": "Velocidad Prom (mi/h)",
            "trip_id": "Trip ID",
            "route_id": "Ruta",
            "speed_max": "Velocidad Máx (mi/h)"
        }
    ).style.format({
        "Velocidad Prom (mi/h)": "{:.2f}",
        "Velocidad Máx (mi/h)": "{:.2f}"
    }))

    st.columns([1, 2], gap = 'medium', vertical_alignment='bottom')

    with col1:
        # ============================================
        # 5️⃣ Actividad de vehículos durante el día
        # ============================================
        st.subheader("⏰ Actividad horaria de la flota")

        activity_hour = df.groupby("hour")["vehicle_id"].nunique().reset_index(name="vehículos activos")
        fig_hour_activity = px.line(
            activity_hour,
            x="hour",
            y="vehículos activos",
            markers=True,
            title="Número de vehículos activos por hora del día",
        )
        fig_hour_activity.update_layout(xaxis_title="Hora del día", yaxis_title="Vehículos activos")
        st.plotly_chart(fig_hour_activity, width=700, use_container_width=True)

    with col2:
        # ============================================
        # 6️⃣ Velocidad promedio por hora
        # ============================================
        st.subheader("📈 Velocidad promedio por hora del día")

        df_speed_hour = (
            df.groupby("hour")["speed"].mean().reset_index()
        )
        fig_speed_hour = px.line(
            df_speed_hour,
            x="hour",
            y="speed",
            markers=True,
            title="Velocidad promedio por hora del día (mi/h)",
        )
        fig_speed_hour.update_layout(xaxis_title="Hora del día", yaxis_title="Velocidad promedio (mi/h)")
        st.plotly_chart(fig_speed_hour, width=700, use_container_width=True)

except Exception as e:
    st.error(f"❌ Error al conectar con la base de datos: {e}")
    st.info("💡 Asegúrate de que:")
    st.markdown("""
    1. PostgreSQL esté corriendo
    2. La base de datos 'transit_streaming' exista
    3. El script de ingesta esté activo
    4. Las credenciales sean correctas
    """)