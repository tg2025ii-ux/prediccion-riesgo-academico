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

# Importar el procesador de datos
from data_processor import DataProcessor

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
    "primary": "#003D3D",      # Verde oscuro (Cajicá)
    "secondary": "#5B9FA0",    # Azul turquesa (Chía)
    "accent": "#F4E85A",       # Amarillo
    "background": "#262730",   # Fondo
    "text": "#FFFFFF",
    "success": "#5B9FA0",
    "warning": "#F4E85A",
    "danger": "#D32F2F"
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
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
    }}
    .metric-card h1, .metric-card h2, .metric-card h3, .metric-card h4, .metric-card p {{
        color: #000000 !important;  /* ← AGREGA ESTA LÍNEA */
    }}
    .risk-high {{
        background-color: #FFEBEE;
        border-left: 4px solid {COLORS['danger']};
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
    }}
    .risk-medium {{
        background-color: #FFF9C4;
        border-left: 4px solid {COLORS['warning']};
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
    }}
    .risk-low {{
        background-color: #E8F5E9;
        border-left: 4px solid {COLORS['success']};
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
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
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
    }}
    .warning-message {{
        padding: 1rem;
        background-color: #FFF9C4;
        border-left: 4px solid {COLORS['warning']};
        border-radius: 5px;
        margin: 1rem 0;
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
    }}
    .error-message {{
        padding: 1rem;
        background-color: #FFEBEE;
        border-left: 4px solid {COLORS['danger']};
        border-radius: 5px;
        margin: 1rem 0;
        color: #000000;  /* ← AGREGA ESTA LÍNEA */
    }}
    </style>
""", unsafe_allow_html=True)

# Inicializar el procesador
@st.cache_resource
def get_processor():
    return DataProcessor()

processor = get_processor()

# Sidebar
with st.sidebar:
    st.image("image.png", use_container_width=True)
    st.markdown("---")
    
    st.markdown(f"""
    <div style='padding: 1rem; background-color: #262730; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);'>
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
                <li>Origen geográfic</li>
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

elif menu == "📤 Cargar Datos":
    st.title("📤 Cargar Datos de Estudiantes")
    
    st.markdown(f"""
    <div class='warning-message'>
        <h4>⚠️ Importante</h4>
        <p>Antes de cargar tus datos, asegúrate de descargar y completar la plantilla oficial.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Botón de descarga de plantilla
    col1, col2 = st.columns([1, 2])
    
with col1:
    st.markdown("### 📥 Paso 1: Descarga la Plantilla")

    # Ruta del instructivo PDF
    instructivo_path = os.path.join(os.path.dirname(__file__), "Instructivo.pdf")
    try:
        with open(instructivo_path, "rb") as file:
            st.download_button(
                label="⬇️ Descargar Instructivo Plantilla",
                data=file,
                file_name="Instructivo.pdf",
                mime="application/pdf",
                use_container_width=True
            )
    except Exception as e:
        st.warning(f"El instructivo no está disponible en este momento. Error: {str(e)}")

    # Ruta de la plantilla Excel
    plantilla_path = os.path.join(os.path.dirname(__file__), "Plantilla.xlsm")
    try:
        with open(plantilla_path, "rb") as file:
            st.download_button(
                label="⬇️ Descargar Plantilla",
                data=file,
                file_name="Plantilla.xlsm",
                mime="application/vnd.ms-excel.sheet.macroEnabled.12",
                use_container_width=True
            )
    except Exception as e:
        st.warning(f"La plantilla no está disponible en este momento. Error: {str(e)}")
    
    with col2:
        st.markdown("""
        **Instrucciones:**
        1. Descarga la plantilla Excel
        2. Completa la información de cada estudiante
        3. Guarda el archivo
        
        **Nota 1:** La plantilla incluye una macro que genera el archivo `Estudiantes_Limpio.xlsx`. Debes activar los permisos para el uso de macros 
        **Nota 2:** Si ya se decargó la plantilla no es necesario volver a descargarla
        """)
    
    st.markdown("---")

    with col1:
    st.markdown("### 📤 Paso 2: Carga tu Archivo")
    
    uploaded_file = st.file_uploader(
        "Sube el archivo Estudiantes_Limpio.xlsx",
        type=['xlsx', 'xls'],
        help="Archivo generado por la plantilla con datos de estudiantes"
    )

    with col2:
        st.markdown("""
        **Instrucciones:**
        1. Sube el archivo `Estudiantes_Limpio.xlsx`
        2. Sí el archivio es válido puedes continuar. En caso contario, verifica el cargado
        3. Da clic en "Procesar Datos"
        4. Sí desea puede previsualizar los datos ingresados
        5. Sí el proceso se ejecuto correctamente aparecerá un mensaje de confirmación
            
    if uploaded_file is not None:
        with st.spinner("🔄 Procesando archivo..."):
            try:
                # Leer el archivo
                df = pd.read_excel(uploaded_file)
                
                # Validar datos
                is_valid, errors = processor.validate_data(df)
                
                if is_valid:
                    st.markdown(f"""
                    <div class='success-message'>
                        <h4> Archivo válido</h4>
                        <p>Se encontraron <b>{len(df)}</b> estudiantes para analizar.</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Mostrar preview
                    with st.expander("👁️ Ver vista previa de los datos"):
                        st.dataframe(df.head(10), use_container_width=True)
                    
                    # Procesar datos
                    if st.button("🚀 Procesar Datos", use_container_width=True):
                        with st.spinner("⚙️ Aplicando modelo predictivo..."):
                            processed_df = processor.process_data(df)
                            st.session_state['processed_data'] = processed_df
                            st.session_state['upload_time'] = datetime.now()
                            
                            st.markdown(f"""
                            <div class='success-message'>
                                <h4>🎉 ¡Procesamiento completado!</h4>
                                <p>Los resultados están listos. Ve a la sección <b>📊 Resultados</b> para visualizarlos.</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.balloons()
                
                else:
                    st.markdown(f"""
                    <div class='error-message'>
                        <h4>❌ Error en el archivo</h4>
                        <p>El archivo no cumple con el formato requerido:</p>
                        <ul>
                            {''.join([f'<li>{error}</li>' for error in errors])}
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
            
            except Exception as e:
                st.markdown(f"""
                <div class='error-message'>
                    <h4>❌ Error al procesar el archivo</h4>
                    <p>{str(e)}</p>
                    <p>Por favor, verifica que el archivo sea correcto y vuelve a intentarlo.</p>
                </div>
                """, unsafe_allow_html=True)

elif menu == "📊 Resultados":
    st.title("📊 Resultados del Análisis")
    
    if 'processed_data' not in st.session_state:
        st.markdown(f"""
        <div class='warning-message'>
            <h4>⚠️ No hay datos procesados</h4>
            <p>Por favor, carga y procesa un archivo primero en la sección <b>📤 Cargar Datos</b>.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        df = st.session_state['processed_data']
        stats = processor.get_summary_stats(df)
        
        # Resumen general
        st.markdown("### 📈 Resumen General")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "👥 Total Estudiantes",
                f"{stats['total_estudiantes']:,}",
                help="Número total de estudiantes analizados"
            )
        
        with col2:
            st.metric(
                "🟢 Riesgo Bajo",
                f"{stats['riesgo_bajo']:,}",
                f"{(stats['riesgo_bajo']/stats['total_estudiantes']*100):.1f}%"
            )
        
        with col3:
            st.metric(
                "🟡 Riesgo Medio",
                f"{stats['riesgo_medio']:,}",
                f"{(stats['riesgo_medio']/stats['total_estudiantes']*100):.1f}%"
            )
        
        with col4:
            st.metric(
                "🔴 Riesgo Alto",
                f"{stats['riesgo_alto']:,}",
                f"{(stats['riesgo_alto']/stats['total_estudiantes']*100):.1f}%"
            )
        
        st.markdown("---")
        
        # Gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🎯 Distribución de Riesgo")
            
            # Gráfico de pastel
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
            st.markdown("### 📊 Distribución de Probabilidades")
            
            # Histograma
            fig = px.histogram(
                df,
                x='probabilidad',
                nbins=30,
                labels={'probabilidad': 'Probabilidad de Riesgo'},
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
        
        # Tabla de resultados con filtros
        st.markdown("### 📋 Detalles de Estudiantes")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            filter_risk = st.multiselect(
                "Filtrar por Nivel de Riesgo",
                options=["Bajo", "Medio", "Alto"],
                default=["Alto"]
            )
        
        # Aplicar filtros
        filtered_df = df[df['nivel_riesgo'].isin(filter_risk)] if filter_risk else df
        
        # Seleccionar columnas importantes para mostrar
        display_columns = ['Ciclo', 'Mult Programa', 'Situacion Acad', 'Promedio Acumulado', 
                          'Sexo', 'probabilidad', 'nivel_riesgo']
        
        # Crear columna de color
        def get_risk_color(nivel):
            if nivel == "Alto":
                return "🔴"
            elif nivel == "Medio":
                return "🟡"
            else:
                return "🟢"
        
        filtered_df['🚦'] = filtered_df['nivel_riesgo'].apply(get_risk_color)
        
        display_df = filtered_df[['🚦'] + [col for col in display_columns if col in filtered_df.columns]]
        
        st.dataframe(
            display_df.style.format({'probabilidad': '{:.2%}'}),
            use_container_width=True,
            height=400
        )
        
        st.markdown("---")
        
        # Descargas
        st.markdown("### 💾 Descargar Resultados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Excel con resultados completos
            output_file = f"Resultados_Riesgo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            @st.cache_data
            def convert_df_to_excel(df):
                from io import BytesIO
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Resultados')
                return output.getvalue()
            
            excel_data = convert_df_to_excel(df)
            
            st.download_button(
                label="📥 Descargar Excel Completo",
                data=excel_data,
                file_name=output_file,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        with col2:
            # CSV
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar CSV",
                data=csv,
                file_name=f"Resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
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
