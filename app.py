# -*- coding: utf-8 -*-
"""
Aplicación Streamlit para Predicción de Riesgo Académico
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os
from pipeline_integrado import ejecutar_pipeline_streamlit, validar_excel

# Importar el procesador de datos
from data_processor_xgboost import DataProcessorXGBoost

# Configuración de la página
st.set_page_config(
    page_title="Predicción de Riesgo Académico",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Forzar consistencia visual
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif !important;
        font-size: 16px !important;
    }
    
    .main .block-container {
        max-width: 1200px;
        padding: 2rem 3rem;
    }
    
    /* Prevenir ajustes automáticos de texto */
    * {
        -webkit-text-size-adjust: 100%;
        text-size-adjust: 100%;
    }
    </style>
""", unsafe_allow_html=True)

# Colores institucionales
COLORS = {
    "primary": "#FFFFFF",      # Verde oscuro (SÓLIDO)
    "secondary": "#8B9D83",    # Verde claro (ARENA)
    "accent": "#5C6B5E",       # Verde medio (SERENO)
    "background": "#FAFAFA",   # Fondo
    "text": "#FFFFFF",         # Texto oscuro
    "success": "#8B9D83",      # Verde claro para éxitos
    "warning": "#5C6B5E",      # Verde medio para advertencias
    "danger": "#3A4A3D"        # Verde oscuro para peligros/errores
}

# CSS personalizado
st.markdown(f"""
    <style>
    .main {{
        background-color: {COLORS['background']};
    }}
    .stButton>button {{
        background-color: {COLORS['primary']};
        color: white;
        font-weight: bold;
        border-radius: 8px;
        padding: 0.5rem 2rem;
        border: none;
        transition: all 0.3s;
    }}
    .stButton>button:hover {{
        background-color: {COLORS['secondary']};
        transform: scale(1.05);
    }}
    .metric-card {{
        background-color: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid {COLORS['primary']};
        color: #000000;
    }}
    .metric-card h1, .metric-card h2, .metric-card h3, .metric-card h4, .metric-card p {{
        color: #000000 !important;
    }}
    .risk-high {{
        background-color: #FFEBEE;
        border-left: 4px solid {COLORS['danger']};
        color: #000000;
    }}
    .risk-medium {{
        background-color: #FFF9C4;
        border-left: 4px solid {COLORS['warning']};
        color: #000000;
    }}
    .risk-low {{
        background-color: #E8F5E9;
        border-left: 4px solid {COLORS['success']};
        color: #000000;
    }}
    h1 {{
        color: {COLORS['primary']};
    }}
    h2, h3 {{
        color: {COLORS['secondary']};
    }}
    .success-message {{
        padding: 1rem;
        background-color: #E8F5E9;
        border-left: 4px solid {COLORS['success']};
        border-radius: 5px;
        margin: 1rem 0;
        color: #000000;
    }}
    .warning-message {{
        padding: 1rem;
        background-color: #FFF9C4;
        border-left: 4px solid {COLORS['warning']};
        border-radius: 5px;
        margin: 1rem 0;
        color: #000000;
    }}
    .error-message {{
        padding: 1rem;
        background-color: #FFEBEE;
        border-left: 4px solid {COLORS['danger']};
        border-radius: 5px;
        margin: 1rem 0;
        color: #000000;
    }}
    </style>
""", unsafe_allow_html=True)

# Inicializar el procesador
@st.cache_resource
def get_processor():
    return DataProcessorXGBoost()  

processor = DataProcessorXGBoost()

# Sidebar
with st.sidebar:
    st.image("image.png", use_container_width=True)
    st.markdown("---")
    
    st.markdown(f"""
    <div style='padding: 1rem; background-color: {COLORS['text']}; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);'>
        <h3 style='color: {COLORS['danger']}; margin-top: 0;'>📊 Sistema de Predicción</h3>
        <p style='color: {COLORS['danger']}; margin-bottom: 0;'>
        Herramienta de análisis predictivo para identificar estudiantes en riesgo académico.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    menu = st.radio(
        "Navegación",
        ["🏠 Inicio", "📤 Cargar Datos", "📊 Resultados", "ℹ️ Ayuda"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.markdown(f"""
    <div style='text-align: center; color: {COLORS['text']}; font-size: 0.8rem;'>
        <p><b>Proyecto de Grado</b></p>
        <p>Universidad Javeriana</p>
        <p>{datetime.now().year}</p>
    </div>
    """, unsafe_allow_html=True)

# Página principal
if menu == "🏠 Inicio":
    st.title("🎓 Sistema de alerta de deserción universitaria")
    
    st.markdown(f"""
    <div class='success-message'>
        <h3>👋 ¡Bienvenid@!</h3>
        <p>Esta herramienta utiliza <b>Machine Learning</b> para predecir el riesgo de deserción de estudiantes
        basándose en múltiples factores como:</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class='metric-card'>
            <h4>📚 Académico</h4>
            <ul>
                <li>Promedio Académico</li>
                <li>Situación Académica</li>
                <li>Categorías de Clases</li>
                <li>Ciclo de Admisión</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class='metric-card'>
            <h4>👤 Personal</h4>
            <ul>
                <li>Edad</li>
                <li>Sexo</li>
                <li>Origen geográfico</li>
                <li>Ciudad de residencia</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class='metric-card'>
            <h4>📈 Rendimiento</h4>
            <ul>
                <li>Calificaciones del ciclo</li>
                <li>Materias perdidas</li>
                <li>Promedio Acumulado</li>
                <li>Créditos Aprobados</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
      
    st.markdown("---")
    
    st.markdown("### 🎯 Niveles de Riesgo")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class='metric-card risk-low'>
            <h4>🟢 Riesgo Bajo</h4>
            <p><b>Probabilidad < 30%</b></p>
            <p>Estudiante con desempeño satisfactorio. Continuar con seguimiento regular.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class='metric-card risk-medium'>
            <h4>🟡 Riesgo Medio</h4>
            <p><b>Probabilidad 30-60%</b></p>
            <p>Requiere atención. Considerar tutorías o acompañamiento académico.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class='metric-card risk-high'>
            <h4>🔴 Riesgo Alto</h4>
            <p><b>Probabilidad > 60%</b></p>
            <p>Requiere intervención inmediata. Apoyo prioritario necesario.</p>
        </div>
        """, unsafe_allow_html=True)

elif menu == "📤 Cargar Datos":
    st.title("📤 Cargar Bases de Datos de la Universidad")
    # Instrucciones
    st.markdown(f"""
    <div style='padding: 1.5rem; background-color: #FFF9C4; border-radius: 10px; border-left: 4px solid {COLORS['warning']}; margin-bottom: 2rem;'>
        <h3 style='color: {COLORS['danger']}; margin-top: 0;'>📋 Instrucciones de Carga</h3>
        <p style='color: {COLORS['danger']}; margin-bottom: 0.5rem;'>
        Debes cargar <b>UN archivo Excel</b> que contenga <b>4 hojas (sheets)</b> con los siguientes nombres:
        </p>
        <ul style='color: {COLORS['danger']}; margin-bottom: 0;'>
            <li><b>NOTAS</b> - Calificaciones y materias de estudiantes</li>
            <li><b>PER</b> - Información personal de estudiantes</li>
            <li><b>PROM</b> - Promedios académicos</li>
            <li><b>ADM</b> - Datos de admisión</li>
        </ul>
        <p style='color: {COLORS['danger']}; margin-top: 1rem; margin-bottom: 0;'>
        <b>IMPORTANTE:</b> Los nombres de las hojas deben ser exactamente como se muestran arriba.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Upload del archivo
    st.markdown("### 📂 Seleccionar Archivo")
    
    uploaded_file = st.file_uploader(
        "Sube tu archivo Excel con las 4 hojas",
        type=['xlsx', 'xls'],
        help="El archivo debe contener las hojas: NOTAS, PER, PROM, ADM",
        key="excel_upload"
    )
    
    if uploaded_file is not None:
        # Validar archivo
        with st.spinner("🔍 Validando archivo..."):
            es_valido, mensaje, dfs = validar_excel(uploaded_file)
        
        if not es_valido:
            st.error(mensaje)
        else:
            # Mostrar información del archivo
            st.success("✅ Archivo cargado correctamente")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"""
                **📁 Archivo:** {uploaded_file.name}  
                **📊 Tamaño:** {uploaded_file.size / 1024:.1f} KB
                """)
            
            with col2:
                st.info(f"""
                **📋 NOTAS:** {len(dfs['NOTAS']):,} registros  
                **👤 PER:** {len(dfs['PER']):,} registros  
                **📈 PROM:** {len(dfs['PROM']):,} registros  
                **🎓 ADM:** {len(dfs['ADM']):,} registros
                """)
            
            # Vista previa de las hojas
            st.markdown("---")
            st.markdown("### 👁️ Vista Previa de los Datos")
            
            tab1, tab2, tab3, tab4 = st.tabs(["📋 NOTAS", "👤 PER", "📈 PROM", "🎓 ADM"])
            
            with tab1:
                st.dataframe(dfs['NOTAS'].head(100), use_container_width=True, height=300)
                st.caption(f"Mostrando primeras 100 filas de {len(dfs['NOTAS']):,} registros")
            
            with tab2:
                st.dataframe(dfs['PER'].head(100), use_container_width=True, height=300)
                st.caption(f"Mostrando primeras 100 filas de {len(dfs['PER']):,} registros")
            
            with tab3:
                st.dataframe(dfs['PROM'].head(100), use_container_width=True, height=300)
                st.caption(f"Mostrando primeras 100 filas de {len(dfs['PROM']):,} registros")
            
            with tab4:
                st.dataframe(dfs['ADM'].head(100), use_container_width=True, height=300)
                st.caption(f"Mostrando primeras 100 filas de {len(dfs['ADM']):,} registros")
            
            # Botón de procesamiento
            st.markdown("---")
            st.markdown("### 🚀 Procesar y Predecir")
            
            st.info("""
            **El sistema ejecutará automáticamente 4 pasos:**
            
            1. 🧹 **Limpieza** (10 fases): Consolidación, filtros, merge de bases
            2. 🎨 **Encoding** (11 fases): Dumificación, transformaciones
            3. 🔧 **Ajustes** (12 fases): Dropout corrida, columnas finales
            4. 🤖 **Predicción XGBoost**: Probabilidades de deserción usando el modelo entrenado
            
            ⏱️ **Tiempo estimado:** 30-60 segundos según tamaño de datos
            """)
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col2:
                if st.button("🚀 PROCESAR Y PREDECIR", type="primary", use_container_width=True):
                    try:
                        st.markdown("---")
                        
                        # Contenedor para el proceso
                        with st.container():
                            # ============================================================
                            # PASO 1-3: PIPELINE INTEGRADO (Limpieza + Encoding + Ajustes)
                            # ============================================================
                            
                            st.markdown(f"""
                            <div style='padding: 1rem; background-color: {COLORS['text']}; border-radius: 10px; border-left: 4px solid {COLORS['text']};'>
                                <h4 style='color: {COLORS['danger']}; margin: 0;'>⏳ Procesando datos (Limpieza → Encoding → Ajustes)...</h4>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Ejecutar pipeline integrado con barra de progreso
                            data_procesada = ejecutar_pipeline_streamlit(
                                dfs['NOTAS'],
                                dfs['PER'],
                                dfs['PROM'],
                                dfs['ADM']
                            )
                            
                            st.success("✅ Pipeline completado!")
                            
                            # Mostrar info de datos procesados
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("📝 Registros", f"{len(data_procesada):,}")
                            with col2:
                                st.metric("📊 Columnas", f"{len(data_procesada.columns)}")
                            with col3:
                                if 'desercion' in data_procesada.columns:
                                    st.metric("⚠️ Con deserción", f"{(data_procesada['desercion']==1).sum():,}")
                            
                            # ============================================================
                            # PASO 4: PREDICCIÓN CON XGBOOST
                            # ============================================================
                            
                            st.markdown("---")
                            st.markdown(f"""
                            <div style='padding: 1rem; background-color: {COLORS['background']}; border-radius: 10px; border-left: 4px solid {COLORS['primary']}; margin-top: 1rem;'>
                                <h4 style='color: {COLORS['danger']}; margin: 0;'>🤖 Ejecutando predicción con XGBoost...</h4>
                                <p style='color: {COLORS['danger']}; margin-top: 0.5rem; margin-bottom: 0;'>Usando modelo: xgboost_modelo.pkl</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            with st.spinner("🤖 Generando predicciones..."):
                                # AQUÍ SE USA xgboost_modelo.pkl
                                resultados = processor.predecir_procesado(data_procesada)
                                
                                st.success("✅ ¡Predicción completada!")
                                st.balloons()
                            
                            # Guardar en session_state
                            st.session_state['processed_data'] = resultados
                            st.session_state['resultados'] = resultados
                            st.session_state['data_procesada'] = data_procesada
                            st.session_state['archivo_cargado'] = uploaded_file.name
                            st.session_state['upload_time'] = datetime.now()
                            
                            # ============================================================
                            # RESUMEN DE RESULTADOS
                            # ============================================================
                            
                            st.markdown("---")
                            st.markdown("### 📊 Resumen de Resultados")
                            
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric(
                                    "📝 Estudiantes",
                                    f"{len(resultados):,}"
                                )
                            
                            with col2:
                                if 'probabilidad' in resultados.columns:
                                    prob_promedio = resultados['probabilidad'].mean()
                                    st.metric(
                                        "📊 Riesgo Promedio",
                                        f"{prob_promedio:.1%}"
                                    )
                            
                            with col3:
                                if 'nivel_riesgo' in resultados.columns:
                                    riesgo_alto = (resultados['nivel_riesgo'] == 'Alto').sum()
                                    st.metric(
                                        "🔴 Riesgo Alto",
                                        f"{riesgo_alto:,}",
                                        delta=f"{(riesgo_alto/len(resultados)*100):.1f}%"
                                    )
                            
                            with col4:
                                if 'nivel_riesgo' in resultados.columns:
                                    riesgo_bajo = (resultados['nivel_riesgo'] == 'Bajo').sum()
                                    st.metric(
                                        "🟢 Riesgo Bajo",
                                        f"{riesgo_bajo:,}",
                                        delta=f"{(riesgo_bajo/len(resultados)*100):.1f}%"
                                    )
                            
                            # Gráfico rápido de distribución
                            if 'nivel_riesgo' in resultados.columns:
                                st.markdown("---")
                                st.markdown("#### 📈 Distribución de Riesgo")
                                
                                import plotly.graph_objects as go
                                
                                distribucion = resultados['nivel_riesgo'].value_counts()
                                
                                fig = go.Figure(data=[
                                    go.Bar(
                                        x=distribucion.index,
                                        y=distribucion.values,
                                        marker_color=['#4CAF50', '#FFC107', '#F44336'],
                                        text=distribucion.values,
                                        textposition='auto',
                                    )
                                ])
                                
                                fig.update_layout(
                                    title="Cantidad de Estudiantes por Nivel de Riesgo",
                                    xaxis_title="Nivel de Riesgo",
                                    yaxis_title="Cantidad de Estudiantes",
                                    height=400,
                                    showlegend=False
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            
                            # Mensaje final
                            st.markdown("---")
                            
                            st.success("""
                            ✅ **¡Proceso completado exitosamente!**
                            
                            **Próximos pasos:**
                            1. Ve a la sección **📊 Resultados** en el menú lateral
                            2. Explora el dashboard interactivo con gráficos y análisis
                            3. Descarga los resultados en Excel o CSV
                            
                            El modelo XGBoost ha analizado {count:,} estudiantes y calculado sus probabilidades de deserción.
                            """.format(count=len(resultados)))
                    
                    except Exception as e:
                        st.error(f"❌ Error durante el procesamiento: {str(e)}")
                        
                        with st.expander("🔍 Ver detalles del error (para debugging)"):
                            import traceback
                            st.code(traceback.format_exc())
                        
                        st.warning("""
                        **Posibles causas del error:**
                        
                        1. **Datos faltantes**: Verifica que todas las hojas tengan las columnas requeridas
                        2. **Formato incorrecto**: Asegúrate de que los datos estén en el formato esperado
                        3. **Modelo no cargado**: Verifica que `xgboost_modelo.pkl` esté en la raíz del proyecto
                        4. **Archivos de configuración**: Verifica que `Libro1.xlsx` y `columnas.csv` existan
                        
                        **Soluciones:**
                        - Revisa los logs en la consola
                        - Verifica que todos los archivos necesarios estén en el repositorio
                        - Contacta al administrador si el problema persiste
                        """)
    
    else:
        # Mensaje cuando no hay archivo cargado
        st.info("""
        👆 **Sube tu archivo Excel para comenzar**
        
        El sistema procesará automáticamente:
        - Limpieza de datos
        - Encoding de variables
        - Ajustes finales
        - Predicción con modelo XGBoost
        """)
elif menu == "📊 Resultados":
    st.title("📊 Resultados del Análisis Predictivo")
    
    if 'processed_data' not in st.session_state:
        st.markdown(f"""
        <div class='warning-message'>
            <h4>⚠️ No hay datos procesados</h4>
            <p>Por favor, carga y procesa los archivos primero en la sección <b>📤 Cargar Datos</b>.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        df = st.session_state['processed_data']
        
        # Verificar que tiene las columnas necesarias
        if 'probabilidad' not in df.columns or 'nivel_riesgo' not in df.columns:
            st.error("❌ Los datos no tienen predicciones. Vuelve a procesar los datos.")
        else:
            # ====================================================================
            # MÉTRICAS PRINCIPALES
            # ====================================================================
            st.markdown("### 📊 Resumen General")
            
            col1, col2, col3, col4 = st.columns(4)
            
            total = len(df)
            prob_promedio = df['probabilidad'].mean()
            riesgo_alto = (df['nivel_riesgo'] == 'Alto').sum()
            riesgo_bajo = (df['nivel_riesgo'] == 'Bajo').sum()
            
            with col1:
                st.metric(
                    "👥 Total Estudiantes",
                    f"{total:,}",
                    help="Total de estudiantes analizados"
                )
            
            with col2:
                st.metric(
                    "📊 Riesgo Promedio",
                    f"{prob_promedio:.1%}",
                    help="Probabilidad promedio de deserción"
                )
            
            with col3:
                pct_alto = (riesgo_alto / total * 100)
                st.metric(
                    "🔴 Riesgo Alto",
                    f"{riesgo_alto:,}",
                    f"{pct_alto:.1f}%",
                    delta_color="inverse",
                    help="Estudiantes con probabilidad > 60%"
                )
            
            with col4:
                pct_bajo = (riesgo_bajo / total * 100)
                st.metric(
                    "🟢 Riesgo Bajo",
                    f"{riesgo_bajo:,}",
                    f"{pct_bajo:.1f}%",
                    help="Estudiantes con probabilidad < 30%"
                )
            
            st.markdown("---")
            
            # ====================================================================
            # TABS PARA ORGANIZAR CONTENIDO
            # ====================================================================
            tab1, tab2, tab3, tab4 = st.tabs([
                "📈 Distribución General",
                "⚖️ Análisis de Equidad",
                "📋 Tabla Detallada",
                "💾 Descargas"
            ])
            
            # ================================================================
            # TAB 1: DISTRIBUCIÓN GENERAL
            # ================================================================
            with tab1:
                st.markdown("### 📈 Distribución de Riesgo")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Gráfico de pastel
                    st.markdown("#### 🎯 Distribución por Nivel")
                    
                    risk_counts = df['nivel_riesgo'].value_counts()
                    
                    fig = go.Figure(data=[go.Pie(
                        labels=risk_counts.index,
                        values=risk_counts.values,
                        marker=dict(colors=['#4CAF50', '#FFC107', '#F44336']),
                        hole=0.4,
                        textinfo='label+percent',
                        textfont=dict(size=14),
                        hovertemplate='<b>%{label}</b><br>%{value} estudiantes<br>%{percent}<extra></extra>'
                    )])
                    
                    fig.update_layout(
                        showlegend=True,
                        height=400,
                        margin=dict(t=30, b=0, l=0, r=0),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=-0.2,
                            xanchor="center",
                            x=0.5
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Gráfico de barras
                    st.markdown("#### 📊 Cantidad por Nivel")
                    
                    fig = go.Figure(data=[
                        go.Bar(
                            x=risk_counts.index,
                            y=risk_counts.values,
                            marker_color=['#4CAF50', '#FFC107', '#F44336'],
                            text=risk_counts.values,
                            textposition='auto',
                            texttemplate='%{text:,}',
                            hovertemplate='<b>%{x}</b><br>%{y:,} estudiantes<extra></extra>'
                        )
                    ])
                    
                    fig.update_layout(
                        xaxis_title="Nivel de Riesgo",
                        yaxis_title="Cantidad de Estudiantes",
                        showlegend=False,
                        height=400,
                        margin=dict(t=30, b=0, l=0, r=0)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                st.markdown("---")
                
                # Histograma de probabilidades
                st.markdown("#### 📊 Distribución de Probabilidades")
                
                fig = go.Figure()
                
                fig.add_trace(go.Histogram(
                    x=df['probabilidad'],
                    nbinsx=30,
                    marker_color=COLORS['primary'],
                    opacity=0.7,
                    name='Probabilidades',
                    hovertemplate='Probabilidad: %{x:.2%}<br>Frecuencia: %{y}<extra></extra>'
                ))
                
                # Agregar líneas verticales para los umbrales
                fig.add_vline(x=0.3, line_dash="dash", line_color="green", 
                             annotation_text="Bajo/Medio (30%)", annotation_position="top")
                fig.add_vline(x=0.6, line_dash="dash", line_color="red", 
                             annotation_text="Medio/Alto (60%)", annotation_position="top")
                
                fig.update_layout(
                    xaxis_title="Probabilidad de Deserción",
                    yaxis_title="Frecuencia",
                    showlegend=False,
                    height=400,
                    xaxis=dict(tickformat='.0%')
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Estadísticas descriptivas
                st.markdown("---")
                st.markdown("#### 📈 Estadísticas Descriptivas")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Mínimo", f"{df['probabilidad'].min():.2%}")
                
                with col2:
                    st.metric("Mediana", f"{df['probabilidad'].median():.2%}")
                
                with col3:
                    st.metric("Promedio", f"{df['probabilidad'].mean():.2%}")
                
                with col4:
                    st.metric("Máximo", f"{df['probabilidad'].max():.2%}")
            
            # ================================================================
            # TAB 2: ANÁLISIS DE EQUIDAD
            # ================================================================
            with tab2:
                st.markdown("### ⚖️ Análisis de Equidad")
                
                st.info("""
                Distribución de probabilidad por caracteristicas personales.
                """)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Análisis por Sexo
                    if 'Sexo' in df.columns:
                        st.markdown("#### 👫 Análisis por Sexo")
                        
                        # Convertir Sexo a etiquetas legibles
                        df_temp = df.copy()
                        df_temp['Sexo_Label'] = df_temp['Sexo'].map({1: 'Masculino', 0: 'Femenino'})
                        
                        fig = go.Figure()
                        
                        for sexo in df_temp['Sexo_Label'].unique():
                            if pd.notna(sexo):
                                datos = df_temp[df_temp['Sexo_Label'] == sexo]['probabilidad']
                                
                                fig.add_trace(go.Box(
                                    y=datos,
                                    name=sexo,
                                    boxmean='sd',
                                    marker_color=COLORS['primary'] if sexo == 'Masculino' else COLORS['secondary'],
                                    hovertemplate='%{y:.2%}<extra></extra>'
                                ))
                        
                        fig.update_layout(
                            yaxis_title="Probabilidad de Deserción",
                            showlegend=True,
                            height=400,
                            yaxis=dict(tickformat='.0%')
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Tabla comparativa
                        sexo_stats = df_temp.groupby('Sexo_Label')['probabilidad'].agg([
                            ('Promedio', 'mean'),
                            ('Cantidad', 'count')
                        ]).reset_index()
                        
                        sexo_stats['Promedio'] = sexo_stats['Promedio'].apply(lambda x: f"{x:.2%}")
                        sexo_stats['Cantidad'] = sexo_stats['Cantidad'].apply(lambda x: f"{x:,}")
                        
                        st.dataframe(sexo_stats, use_container_width=True, hide_index=True)
                    else:
                        st.warning("⚠️ La variable 'Sexo' no está disponible en los datos")
                
                with col2:
                    # Análisis por Beca
                    if 'Benef. Beca' in df.columns:
                        st.markdown("#### 🎓 Análisis por Beneficiario de Beca")
                        
                        df_temp = df.copy()
                        df_temp['Beca_Label'] = df_temp['Benef. Beca'].map({1: 'Con Beca', 0: 'Sin Beca'})
                        
                        fig = go.Figure()
                        
                        for beca in df_temp['Beca_Label'].unique():
                            if pd.notna(beca):
                                datos = df_temp[df_temp['Beca_Label'] == beca]['probabilidad']
                                
                                fig.add_trace(go.Box(
                                    y=datos,
                                    name=beca,
                                    boxmean='sd',
                                    marker_color=COLORS['accent'] if beca == 'Con Beca' else COLORS['warning'],
                                    hovertemplate='%{y:.2%}<extra></extra>'
                                ))
                        
                        fig.update_layout(
                            yaxis_title="Probabilidad de Deserción",
                            showlegend=True,
                            height=400,
                            yaxis=dict(tickformat='.0%')
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Tabla comparativa
                        beca_stats = df_temp.groupby('Beca_Label')['probabilidad'].agg([
                            ('Promedio', 'mean'),
                            ('Cantidad', 'count')
                        ]).reset_index()
                        
                        beca_stats['Promedio'] = beca_stats['Promedio'].apply(lambda x: f"{x:.2%}")
                        beca_stats['Cantidad'] = beca_stats['Cantidad'].apply(lambda x: f"{x:,}")
                        
                        st.dataframe(beca_stats, use_container_width=True, hide_index=True)
                    else:
                        st.warning("⚠️ La variable 'Benef. Beca' no está disponible en los datos")
                
                st.markdown("---")
                
                # Distribución de riesgo por variables protegidas
                st.markdown("#### 📊 Distribución de Riesgo por Grupos")
                
                if 'Sexo' in df.columns:
                    df_temp = df.copy()
                    df_temp['Sexo_Label'] = df_temp['Sexo'].map({1: 'Masculino', 0: 'Femenino'})
                    
                    sexo_riesgo = pd.crosstab(
                        df_temp['Sexo_Label'], 
                        df_temp['nivel_riesgo'],
                        normalize='index'
                    ) * 100
                    
                    fig = go.Figure()
                    
                    colores = {'Bajo': '#4CAF50', 'Medio': '#FFC107', 'Alto': '#F44336'}
                    
                    for nivel in ['Bajo', 'Medio', 'Alto']:
                        if nivel in sexo_riesgo.columns:
                            fig.add_trace(go.Bar(
                                name=nivel,
                                x=sexo_riesgo.index,
                                y=sexo_riesgo[nivel],
                                marker_color=colores[nivel],
                                text=sexo_riesgo[nivel].round(1),
                                texttemplate='%{text}%',
                                textposition='inside',
                                hovertemplate='%{y:.1f}%<extra></extra>'
                            ))
                    
                    fig.update_layout(
                        barmode='stack',
                        title='Distribución de Riesgo por Sexo',
                        xaxis_title="Sexo",
                        yaxis_title="Porcentaje (%)",
                        height=400,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            # ================================================================
            # TAB 3: TABLA DETALLADA
            # ================================================================
            with tab3:
                st.markdown("### 📋 Tabla Detallada de Estudiantes")
                
                # Filtros
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    filter_risk = st.multiselect(
                        "Filtrar por Nivel de Riesgo",
                        options=["Bajo", "Medio", "Alto"],
                        default=["Alto"],
                        help="Selecciona los niveles de riesgo a mostrar"
                    )
                
                with col2:
                    # Rango de probabilidad
                    prob_min = float(df['probabilidad'].min())
                    prob_max = float(df['probabilidad'].max())
                    
                    prob_range = st.slider(
                        "Rango de Probabilidad",
                        min_value=prob_min,
                        max_value=prob_max,
                        value=(prob_min, prob_max),
                        format="%.2f",
                        help="Filtra por rango de probabilidad"
                    )
                
                with col3:
                    # Buscar por índice
                    search_index = st.text_input(
                        "Buscar por Índice",
                        placeholder="Ej: 0, 5, 10",
                        help="Busca estudiantes por su número de fila (0, 1, 2, ...)"
                    )
                
                # Aplicar filtros
                df_filtered = df.copy()
                
                # AGREGAR COLUMNA DE ÍNDICE PRIMERO
                df_filtered = df_filtered.reset_index(drop=True)
                df_filtered.insert(0, 'Índice', df_filtered.index)
                
                # Filtro por nivel de riesgo
                if filter_risk:
                    df_filtered = df_filtered[df_filtered['nivel_riesgo'].isin(filter_risk)]
                
                # Filtro por rango de probabilidad
                df_filtered = df_filtered[
                    (df_filtered['probabilidad'] >= prob_range[0]) &
                    (df_filtered['probabilidad'] <= prob_range[1])
                ]
                
                # Filtro por índice
                if search_index:
                    try:
                        # Permitir búsqueda de múltiples índices separados por coma
                        indices_str = [idx.strip() for idx in search_index.split(',')]
                        indices = []
                        
                        for idx_str in indices_str:
                            if idx_str.isdigit():
                                indices.append(int(idx_str))
                        
                        if indices:
                            # Filtrar por los índices especificados
                            df_filtered = df_filtered[df_filtered['Índice'].isin(indices)]
                            
                            if len(df_filtered) == 0:
                                st.warning(f"⚠️ No se encontraron registros con índice(s): {', '.join(map(str, indices))}")
                        else:
                            st.warning("⚠️ Formato incorrecto. Usa números separados por coma (Ej: 0, 5, 10)")
                    
                    except ValueError:
                        st.warning("⚠️ Error al procesar los índices. Usa solo números (Ej: 0, 5, 10)")
                
                st.info(f"📊 Mostrando {len(df_filtered):,} de {len(df):,} estudiantes")
                
                # Seleccionar columnas a mostrar
                columnas_disponibles = ['Benef. Beca', 'Mult Programa', 'Ciclo', 'Sexo', 
                                       'rango_edad', 'Promedio Acumulado', 'Situacion Acad', 
                                       'probabilidad', 'nivel_riesgo']
                
                display_columns = [c for c in columnas_disponibles if c in df_filtered.columns]
                
                if display_columns:
                    # Crear DataFrame para mostrar
                    df_display = df_filtered[display_columns].copy()
                    
                    # Agregar emoji de riesgo
                    def get_risk_emoji(nivel):
                        if nivel == "Alto":
                            return "🔴"
                        elif nivel == "Medio":
                            return "🟡"
                        else:
                            return "🟢"
                    
                    df_display.insert(0, '🚦', df_display['nivel_riesgo'].apply(get_risk_emoji))
                    
                    # Formatear probabilidad
                    if 'probabilidad' in df_display.columns:
                        df_display['probabilidad'] = df_display['probabilidad'].apply(lambda x: f"{x:.2%}")
                    
                    # Formatear Sexo
                    if 'Sexo' in df_display.columns:
                        df_display['Sexo'] = df_display['Sexo'].map({1: 'M', 0: 'F'})
                    
                    # Formatear Benef. Beca
                    if 'Benef. Beca' in df_display.columns:
                        df_display['Benef. Beca'] = df_display['Benef. Beca'].map({1: 'Sí', 0: 'No'})
                    
                    # Mostrar tabla
                    st.dataframe(
                        df_display,
                        use_container_width=True,
                        height=400
                    )
                    
                    # Tip de ayuda
                    st.caption("""
                    💡 **Tip:** Puedes buscar múltiples estudiantes separando los índices con coma. 
                    Ejemplo: `0, 5, 10, 15` mostrará solo esos 4 estudiantes.
                    """)
                else:
                    st.warning("⚠️ No hay columnas disponibles para mostrar")
            
            # ================================================================
            # TAB 4: DESCARGAS
            # ================================================================
            with tab4:
                st.markdown("### 💾 Descargar Resultados")
                
                st.info("""
                Descarga los resultados completos con todas las predicciones y probabilidades.
                Los archivos incluyen todas las columnas originales más las predicciones del modelo.
                """)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 📊 Excel Completo")
                    
                    from io import BytesIO
                    output = BytesIO()
                    
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='Resultados')
                    
                    excel_data = output.getvalue()
                    
                    st.download_button(
                        label="📥 Descargar Excel",
                        data=excel_data,
                        file_name=f"Predicciones_Desercion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        type="primary"
                    )
                    
                    st.caption(f"Tamaño: {len(excel_data) / 1024:.1f} KB")
                
                with col2:
                    st.markdown("#### 📄 CSV")
                    
                    csv = df.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="📥 Descargar CSV",
                        data=csv,
                        file_name=f"Predicciones_Desercion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                    st.caption(f"Tamaño: {len(csv) / 1024:.1f} KB")
                
                st.markdown("---")
                
                # Resumen para descarga
                st.markdown("#### 📋 Resumen de Contenido")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Registros", f"{len(df):,}")
                
                with col2:
                    st.metric("Columnas", f"{len(df.columns)}")
                
                with col3:
                    st.metric("Tamaño", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
                
                # Lista de columnas incluidas
                with st.expander("📋 Ver columnas incluidas en la descarga"):
                    st.write(", ".join(df.columns.tolist()))

else:  # Ayuda
    st.title("ℹ️ Ayuda y Documentación")
    
    st.markdown("### ❓ Preguntas Frecuentes")
    
    with st.expander("🤔 ¿Qué es el riesgo académico?"):
        st.markdown("""
        El riesgo académico es la **probabilidad** de que un estudiante tenga dificultades 
        para cumplir con los requisitos académicos del programa. Esta herramienta calcula 
        esta probabilidad basándose en múltiples factores históricos y actuales.
        """)
    
    with st.expander("📊 ¿Cómo se calculan las probabilidades?"):
        st.markdown("""
        El modelo utiliza **XGBoost**, una técnica de Machine Learning que:
        
        1. Analiza datos históricos de miles de estudiantes
        2. Identifica patrones y factores de riesgo
        3. Calcula una probabilidad personalizada para cada estudiante
        4. Clasifica el riesgo en tres niveles: Bajo, Medio y Alto
        
        El modelo considera 157 variables diferentes.
        """)
    
    with st.expander("🎯 ¿Qué hacer con los resultados?"):
        st.markdown("""
        **Para estudiantes en Riesgo Alto:**
        - Contactar inmediatamente al estudiante
        - Ofrecer tutorías especializadas
        - Considerar ajuste de carga académica
        - Derivar a bienestar universitario si es necesario
        
        **Para estudiantes en Riesgo Medio:**
        - Monitoreo más frecuente
        - Ofrecer talleres de métodos de estudio
        - Revisar carga académica del próximo semestre
        
        **Para estudiantes en Riesgo Bajo:**
        - Seguimiento regular
        - Mantener motivación y apoyo
        """)
    
    with st.expander("🔒 ¿Los datos son confidenciales?"):
        st.markdown("""
        **Sí, totalmente confidenciales.**
        
        - Los datos se procesan localmente en tu navegador
        - No se almacenan en servidores externos
        - Solo tú tienes acceso a la información
        - Cumple con normativas de protección de datos
        """)
    
    with st.expander("⚠️ ¿Qué hago si hay un error?"):
        st.markdown("""
        Si encuentras un error:
        
        1. Verifica que el archivo Excel tenga el formato correcto
        2. Asegúrate de que todas las columnas requeridas estén presentes
        3. Revisa que no haya valores vacíos en campos obligatorios
        4. Intenta volver a cargar el archivo
        
        Si el problema persiste, contacta con el administrador del sistema.
        """)
    
    st.markdown("---")
    
    st.markdown("### 📧 Contacto y Soporte")
    
    st.info("""
    **Desarrollado por:** Luis Atencio, Natalia Delgado y Alejandra Mesa  
    **Universidad:** Pontificia Universidad Javeriana  
    **Proyecto:** Trabajo de grado 253028    
    **Email:** latencio@javeriana.edu.co, ndelgadog@javeriana.edu.co, malejandramesa@javeriana.edu.co
    """)

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['text']}; padding: 2rem;'>
    <p>Sistema de alerta de deserción temprana | Universidad Javeriana | {datetime.now().year}</p>
    <p style='font-size: 0.8rem;'>Desarrollado con ❤️ usando Streamlit y Machine Learning</p>
</div>
""", unsafe_allow_html=True)
