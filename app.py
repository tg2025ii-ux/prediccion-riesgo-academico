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
    "primary": "#3A4A3D",      # Verde oscuro (SÓLIDO)
    "secondary": "#8B9D83",    # Verde claro (ARENA)
    "accent": "#5C6B5E",       # Verde medio (SERENO)
    "background": "#FAFAFA",   # Fondo
    "text": "#2A3A2D",         # Texto oscuro
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
    <div style='padding: 1rem; background-color: {COLORS['background']}; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);'>
        <h3 style='color: {COLORS['text']}; margin-top: 0;'>📊 Sistema de Predicción</h3>
        <p style='color: {COLORS['text']}; margin-bottom: 0;'>
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
    
    st.markdown("### ⚙️ Cómo usar esta herramienta")
    
    steps = """
    1. **📥 Descarga la plantilla** de Excel desde la sección "Cargar Datos"
    2. **✏️ Completa la información** de los estudiantes en la plantilla
    3. **📤 Sube el archivo** completado a la aplicación
    4. **📊 Visualiza los resultados** y predicciones automáticas
    5. **💾 Descarga** el informe con todas las probabilidades calculadas
    """
    
    st.info(steps)
    
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

from pipeline_integrado import ejecutar_pipeline_streamlit, validar_excel

elif menu == "📤 Cargar Datos":
    st.title("📤 Cargar Bases de Datos de la Universidad")
    
    # Instrucciones
    st.markdown(f"""
    <div style='padding: 1.5rem; background-color: #FFF9C4; border-radius: 10px; border-left: 4px solid {COLORS['warning']}; margin-bottom: 2rem;'>
        <h3 style='color: {COLORS['text']}; margin-top: 0;'>📋 Instrucciones de Carga</h3>
        <p style='color: {COLORS['text']}; margin-bottom: 0.5rem;'>
        Debes cargar <b>UN archivo Excel</b> que contenga <b>4 hojas (sheets)</b> con los siguientes nombres:
        </p>
        <ul style='color: {COLORS['text']}; margin-bottom: 0;'>
            <li><b>NOTAS</b> - Calificaciones y materias de estudiantes</li>
            <li><b>PER</b> - Información personal de estudiantes</li>
            <li><b>PROM</b> - Promedios académicos</li>
            <li><b>ADM</b> - Datos de admisión</li>
        </ul>
        <p style='color: {COLORS['text']}; margin-top: 1rem; margin-bottom: 0;'>
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
            st.markdown("### 🚀 Procesar Datos")
            
            st.info("""
            **El procesamiento incluye 4 pasos:**
            
            1. 🧹 **Limpieza** (10 fases): Consolidación, filtros, merge de bases
            2. 🎨 **Encoding** (11 fases): Dumificación, transformaciones
            3. 🔧 **Ajustes** (12 fases): Dropout corrida, columnas finales
            4. 🤖 **Predicción**: Modelo XGBoost ejecuta predicciones
            
            ⏱️ **Tiempo estimado:** 30-60 segundos según tamaño de datos
            """)
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col2:
                if st.button("🚀 PROCESAR Y PREDECIR", type="primary", use_container_width=True):
                    try:
                        st.markdown("---")
                        
                        # Contenedor para logs
                        with st.container():
                            st.markdown(f"""
                            <div style='padding: 1rem; background-color: {COLORS['background']}; border-radius: 10px; border-left: 4px solid {COLORS['primary']};'>
                                <h4 style='color: {COLORS['text']}; margin: 0;'>⏳ Procesando datos...</h4>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Ejecutar pipeline con barra de progreso
                            data_procesada = ejecutar_pipeline_streamlit(
                                dfs['NOTAS'],
                                dfs['PER'],
                                dfs['PROM'],
                                dfs['ADM']
                            )
                            
                            st.success("✅ Datos procesados correctamente!")
                            
                            # Guardar en session_state
                            st.session_state['data_procesada'] = data_procesada
                            st.session_state['archivo_cargado'] = uploaded_file.name
                            
                            # Mostrar resumen
                            st.markdown("---")
                            st.markdown("### 📊 Resumen del Procesamiento")
                            
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric(
                                    "📝 Registros Procesados",
                                    f"{len(data_procesada):,}"
                                )
                            
                            with col2:
                                st.metric(
                                    "📊 Variables Totales",
                                    f"{len(data_procesada.columns)}"
                                )
                            
                            with col3:
                                if 'desercion' in data_procesada.columns:
                                    desercion_count = (data_procesada['desercion'] == 1).sum()
                                    st.metric(
                                        "⚠️ Con Deserción",
                                        f"{desercion_count:,}"
                                    )
                                else:
                                    st.metric("⚠️ Deserción", "N/A")
                            
                            # Mostrar vista previa
                            st.markdown("---")
                            st.markdown("### 👁️ Vista Previa de Datos Procesados")
                            st.dataframe(
                                data_procesada.head(50),
                                use_container_width=True,
                                height=300
                            )
                            
                            # Botón para ir a predicción
                            st.markdown("---")
                            
                            st.success("""
                            ✅ **¡Datos listos para predicción!**
                            
                            Ahora ejecutaremos el modelo XGBoost para predecir riesgo de deserción.
                            """)
                            
                            col1, col2, col3 = st.columns([1, 2, 1])
                            with col2:
                                if st.button("🤖 EJECUTAR PREDICCIÓN", type="primary", use_container_width=True):
                                    with st.spinner("🤖 Ejecutando modelo XGBoost..."):
                                        # Aquí se ejecuta la predicción
                                        # El código de predicción ya está en tu app original
                                        # Solo necesitamos ejecutarlo con data_procesada
                                        
                                        try:
                                            # Usar el procesador que ya tienes
                                            processor = st.session_state.get('processor', DataProcessorXGBoost())
                                            
                                            # Ejecutar predicción
                                            resultados = processor.predict(data_procesada)
                                            
                                            # Guardar resultados
                                            st.session_state['resultados'] = resultados
                                            st.session_state['data_procesada_completa'] = data_procesada
                                            
                                            st.success("✅ ¡Predicción completada!")
                                            st.balloons()
                                            
                                            st.info("""
                                            **Predicción completada exitosamente!**
                                            
                                            Ve a la sección **📊 Resultados** en el menú lateral para ver el análisis completo.
                                            """)
                                            
                                        except Exception as e:
                                            st.error(f"❌ Error en la predicción: {str(e)}")
                                            st.exception(e)
                    
                    except Exception as e:
                        st.error(f"❌ Error durante el procesamiento: {str(e)}")
                        st.exception(e)
                        
                        st.warning("""
                        **Posibles causas del error:**
                        - Datos faltantes en columnas requeridas
                        - Formato incorrecto en alguna hoja
                        - Valores inesperados en variables clave
                        
                        Revisa los logs anteriores para más detalles.
                        """)
    
    else:
        # Mensaje cuando no hay archivo cargado
        st.info("""
        👆 **Sube tu archivo Excel para comenzar**
        
        El sistema procesará automáticamente los datos y ejecutará el modelo de predicción.
        """)
        
        # Ejemplo visual
        st.markdown("---")
        st.markdown("### 📋 Ejemplo de Estructura del Archivo")
        
        st.image("https://via.placeholder.com/800x400/3A4A3D/FFFFFF?text=Estructura+del+Excel:+4+hojas+(NOTAS,+PER,+PROM,+ADM)", 
                 use_container_width=True,
                 caption="El archivo Excel debe contener exactamente estas 4 hojas")

# SECCIÓN PARA REEMPLAZAR EN APP.PY - RESULTADOS

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
        data_original = st.session_state.get('data_original', {})
        
        # Importar procesador para estadísticas
        from data_processor_xgboost import DataProcessorXGBoost
        processor = DataProcessorXGBoost(model_dir='models')
        stats = processor.get_summary_stats(df)
        
        # TABS para organizar resultados
        tab1, tab2, tab3 = st.tabs([
            "📈 Datos Ingresados", 
            "🎯 Predicciones de Deserción",
            "📋 Tabla Detallada"
        ])
        
        # ====================================================================
        # TAB 1: ESTADÍSTICAS DESCRIPTIVAS DE LOS DATOS INGRESADOS
        # ====================================================================
        with tab1:
            st.markdown("### 📊 Características de los Datos Ingresados")
            
            st.info("""
            Esta sección muestra estadísticas descriptivas de los datos que cargaste 
            desde las bases NOTAS, PER, PROM y ADM **antes** de hacer las predicciones.
            """)
            
            # Métricas generales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "👥 Total Estudiantes",
                    f"{stats['total_estudiantes']:,}",
                    help="Total de estudiantes procesados"
                )
            
            with col2:
                if 'per' in data_original and 'Programa' in data_original['per'].columns:
                    n_programas = data_original['per']['Programa'].nunique()
                    st.metric("🎓 Programas", n_programas)
            
            with col3:
                if 'prom' in data_original and 'Promedio Acumulado' in data_original['prom'].columns:
                    prom_gral = data_original['prom']['Promedio Acumulado'].mean()
                    st.metric("📚 Promedio General", f"{prom_gral:.2f}")
            
            with col4:
                if 'per' in data_original and 'Ciclo' in data_original['per'].columns:
                    ciclos = data_original['per']['Ciclo'].nunique()
                    st.metric("📅 Ciclos", ciclos)
            
            st.markdown("---")
            
            # Gráficas descriptivas
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribución por Sexo
                if 'per' in data_original and 'Sexo' in data_original['per'].columns:
                    st.markdown("#### 👫 Distribución por Sexo")
                    fig = px.pie(
                        data_original['per'], 
                        names='Sexo',
                        title='Distribución por Sexo',
                        color_discrete_sequence=[COLORS['primary'], COLORS['secondary']]
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Distribución de Edad
                if 'per' in data_original and 'Edad' in data_original['per'].columns:
                    st.markdown("#### 📊 Distribución de Edad")
                    fig = px.histogram(
                        data_original['per'], 
                        x='Edad',
                        title='Distribución de Edad',
                        nbins=30,
                        color_discrete_sequence=[COLORS['accent']]
                    )
                    fig.update_layout(xaxis_title="Edad", yaxis_title="Frecuencia")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Top 10 Programas
                if 'adm' in data_original and 'Programa Académico' in data_original['adm'].columns:
                    st.markdown("#### 🎓 Top 10 Programas")
                    top_prog = data_original['adm']['Programa Académico'].value_counts().head(10)
                    fig = px.bar(
                        x=top_prog.values,
                        y=top_prog.index,
                        orientation='h',
                        title='Estudiantes por Programa',
                        color_discrete_sequence=[COLORS['primary']]
                    )
                    fig.update_layout(xaxis_title="Estudiantes", yaxis_title="Programa")
                    st.plotly_chart(fig, use_container_width=True)
                
                # Distribución de Promedios
                if 'prom' in data_original and 'Promedio Acumulado' in data_original['prom'].columns:
                    st.markdown("#### 📈 Distribución de Promedios")
                    fig = px.histogram(
                        data_original['prom'],
                        x='Promedio Acumulado',
                        title='Distribución de Promedios Acumulados',
                        nbins=30,
                        color_discrete_sequence=[COLORS['success']]
                    )
                    fig.update_layout(xaxis_title="Promedio Acumulado", yaxis_title="Frecuencia")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Estadísticas adicionales
            st.markdown("---")
            st.markdown("#### 📋 Resumen Estadístico")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if 'prom' in data_original and 'Promedio Acumulado' in data_original['prom'].columns:
                    st.markdown("**Promedios Académicos:**")
                    prom_stats = data_original['prom']['Promedio Acumulado'].describe()
                    st.dataframe(prom_stats, use_container_width=True)
            
            with col2:
                if 'per' in data_original and 'Edad' in data_original['per'].columns:
                    st.markdown("**Edad:**")
                    edad_stats = data_original['per']['Edad'].describe()
                    st.dataframe(edad_stats, use_container_width=True)
        
        # ====================================================================
        # TAB 2: PREDICCIONES DE DESERCIÓN
        # ====================================================================
        with tab2:
            st.markdown("### 🎯 Resultados de Predicción con XGBoost")
            
            # Métricas del modelo
            st.markdown("#### 🤖 Rendimiento del Modelo")
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.markdown("""
                **Modelo:** XGBoost con Mitigación de Sesgo  
                **Variable Protegida:** Sexo  
                **Técnica de Mitigación:** Exponentiated Gradient
                """)
            
            with col2:
                # Mostrar métricas del modelo (de Metricas.txt)
                metricas_df = pd.DataFrame({
                    'Métrica': ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'ROC-AUC'],
                    'Pre-Mitigación': [0.8688, 0.7042, 0.3724, 0.4872, 0.8707],
                    'Post-Mitigación': [0.8686, 0.6942, 0.3841, 0.4946, 0.8707]
                })
                st.dataframe(metricas_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # Métricas de los resultados actuales
            st.markdown("#### 📊 Resultados de la Predicción")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("👥 Total", f"{stats['total_estudiantes']:,}")
            
            with col2:
                pct_bajo = (stats['riesgo_bajo'] / stats['total_estudiantes'] * 100)
                st.metric(
                    "🟢 Riesgo Bajo",
                    f"{stats['riesgo_bajo']:,}",
                    f"{pct_bajo:.1f}%"
                )
            
            with col3:
                pct_medio = (stats['riesgo_medio'] / stats['total_estudiantes'] * 100)
                st.metric(
                    "🟡 Riesgo Medio",
                    f"{stats['riesgo_medio']:,}",
                    f"{pct_medio:.1f}%"
                )
            
            with col4:
                pct_alto = (stats['riesgo_alto'] / stats['total_estudiantes'] * 100)
                st.metric(
                    "🔴 Riesgo Alto",
                    f"{stats['riesgo_alto']:,}",
                    f"{pct_alto:.1f}%",
                    delta_color="inverse"
                )
            
            st.markdown("---")
            
            # Gráficas de predicción
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 🎯 Distribución de Riesgo")
                
                risk_counts = df['nivel_riesgo'].value_counts()
                fig = go.Figure(data=[go.Pie(
                    labels=risk_counts.index,
                    values=risk_counts.values,
                    marker=dict(colors=[COLORS['success'], COLORS['warning'], COLORS['danger']]),
                    hole=0.4
                )])
                fig.update_layout(
                    showlegend=True,
                    height=400,
                    margin=dict(t=0, b=0, l=0, r=0)
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### 📊 Distribución de Probabilidades")
                
                fig = px.histogram(
                    df,
                    x='probabilidad',
                    nbins=30,
                    title='Probabilidades de Deserción',
                    color_discrete_sequence=[COLORS['secondary']]
                )
                fig.update_layout(
                    showlegend=False,
                    height=400,
                    xaxis_title="Probabilidad",
                    yaxis_title="Frecuencia"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            # Análisis por variables protegidas (Equidad)
            st.markdown("#### ⚖️ Análisis de Equidad - Variables Protegidas")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Riesgo por Sexo
                if 'Sexo' in df.columns:
                    st.markdown("**Probabilidad de Deserción por Sexo**")
                    
                    fig = px.box(
                        df,
                        x='Sexo',
                        y='probabilidad',
                        color='Sexo',
                        title='Distribución de Probabilidades por Sexo',
                        color_discrete_sequence=[COLORS['primary'], COLORS['secondary']]
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Tabla de promedios
                    sexo_stats = df.groupby('Sexo')['probabilidad'].agg(['mean', 'count']).reset_index()
                    sexo_stats.columns = ['Sexo', 'Probabilidad Promedio', 'Cantidad']
                    sexo_stats['Probabilidad Promedio'] = sexo_stats['Probabilidad Promedio'].apply(lambda x: f"{x:.2%}")
                    st.dataframe(sexo_stats, use_container_width=True, hide_index=True)
            
            with col2:
                # Riesgo por Beneficiario de Beca
                if 'Benef. Beca' in df.columns:
                    st.markdown("**Probabilidad por Beneficiario de Beca**")
                    
                    fig = px.box(
                        df,
                        x='Benef. Beca',
                        y='probabilidad',
                        color='Benef. Beca',
                        title='Distribución por Beneficiario de Beca',
                        color_discrete_sequence=[COLORS['accent'], COLORS['warning']]
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Tabla de promedios
                    beca_stats = df.groupby('Benef. Beca')['probabilidad'].agg(['mean', 'count']).reset_index()
                    beca_stats.columns = ['Beneficiario Beca', 'Probabilidad Promedio', 'Cantidad']
                    beca_stats['Probabilidad Promedio'] = beca_stats['Probabilidad Promedio'].apply(lambda x: f"{x:.2%}")
                    st.dataframe(beca_stats, use_container_width=True, hide_index=True)
            
            # Riesgo por Programa (Top 10)
            st.markdown("---")
            st.markdown("#### 🎓 Riesgo de Deserción por Programa (Top 10)")
            
            if 'Programa' in df.columns:
                prog_risk = df.groupby('Programa').agg({
                    'probabilidad': 'mean',
                    'ID': 'count'
                }).reset_index()
                prog_risk.columns = ['Programa', 'Probabilidad Promedio', 'Estudiantes']
                prog_risk = prog_risk.nlargest(10, 'Probabilidad Promedio')
                
                fig = px.bar(
                    prog_risk,
                    x='Probabilidad Promedio',
                    y='Programa',
                    orientation='h',
                    title='Programas con Mayor Riesgo Promedio',
                    color='Probabilidad Promedio',
                    color_continuous_scale=['green', 'yellow', 'red']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # ====================================================================
        # TAB 3: TABLA DETALLADA
        # ====================================================================
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
                if 'Programa' in df.columns:
                    programas_unicos = sorted(df['Programa'].unique())
                    filter_programa = st.multiselect(
                        "Filtrar por Programa",
                        options=programas_unicos,
                        help="Filtra por programa académico"
                    )
                else:
                    filter_programa = []
            
            with col3:
                if 'Sexo' in df.columns:
                    filter_sexo = st.multiselect(
                        "Filtrar por Sexo",
                        options=df['Sexo'].unique(),
                        help="Filtra por sexo"
                    )
                else:
                    filter_sexo = []
            
            # Aplicar filtros
            df_filtered = df.copy()
            
            if filter_risk:
                df_filtered = df_filtered[df_filtered['nivel_riesgo'].isin(filter_risk)]
            
            if filter_programa:
                df_filtered = df_filtered[df_filtered['Programa'].isin(filter_programa)]
            
            if filter_sexo:
                df_filtered = df_filtered[df_filtered['Sexo'].isin(filter_sexo)]
            
            st.info(f"📊 Mostrando {len(df_filtered):,} de {len(df):,} estudiantes")
            
            # Seleccionar columnas a mostrar
            display_columns = ['ID', 'Programa', 'Ciclo', 'Sexo', 'Promedio Acumulado', 
                              'probabilidad', 'nivel_riesgo']
            display_columns = [c for c in display_columns if c in df_filtered.columns]
            
            # Crear indicador visual
            def get_risk_emoji(nivel):
                if nivel == "Alto":
                    return "🔴"
                elif nivel == "Medio":
                    return "🟡"
                else:
                    return "🟢"
            
            df_display = df_filtered[display_columns].copy()
            df_display.insert(0, '🚦', df_display['nivel_riesgo'].apply(get_risk_emoji))
            
            # Formatear probabilidad
            if 'probabilidad' in df_display.columns:
                df_display['probabilidad'] = df_display['probabilidad'].apply(lambda x: f"{x:.2%}")
            
            # Mostrar tabla
            st.dataframe(
                df_display,
                use_container_width=True,
                height=400
            )
            
            st.markdown("---")
            
            # Descargas
            st.markdown("### 💾 Descargar Resultados")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Excel completo
                from io import BytesIO
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Resultados')
                excel_data = output.getvalue()
                
                st.download_button(
                    label="📥 Descargar Excel Completo",
                    data=excel_data,
                    file_name=f"Predicciones_XGBoost_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            with col2:
                # CSV
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Descargar CSV",
                    data=csv,
                    file_name=f"Predicciones_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

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
        El modelo utiliza **Regresión Logística**, una técnica de Machine Learning que:
        
        1. Analiza datos históricos de miles de estudiantes
        2. Identifica patrones y factores de riesgo
        3. Calcula una probabilidad personalizada para cada estudiante
        4. Clasifica el riesgo en tres niveles: Bajo, Medio y Alto
        
        El modelo considera más de 150 variables diferentes.
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
