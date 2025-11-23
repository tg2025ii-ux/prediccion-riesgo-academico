"""
Componente Streamlit para ENCODING COMPLETO
Permite cargar base limpia y descargar base codificada
"""

import streamlit as st
import pandas as pd
import io
from data_processor_encoding import DataProcessorEncoding, procesar_encoding_completo

def seccion_encoding():
    """
    Sección de Streamlit para encoding (dumificación)
    """
    st.header("🎨 ENCODING COMPLETO (11 FASES)")
    st.markdown("""
    Esta sección transforma la base limpia en base codificada aplicando:
    
    **FASE 1: Transformaciones Iniciales**
    - ✅ Benef. Beca → 0/1
    - ✅ Sexo → 0/1
    
    **FASE 2: Eliminar Programas Finalizados**
    - ✅ Identificar y eliminar programas/IDs finalizados
    
    **FASE 3: Crear Estado Dropout**
    - ✅ Mapear estados a variable binaria (0/1)
    
    **FASE 4-11: Dumificación**
    - ✅ Programa → p_*
    - ✅ Siglas Prog → s_*
    - ✅ Ciudad (normalizada) → cd_* (solo Bogotá D.C.)
    - ✅ Dpto Nacimiento → dn_* (sin Otro/Ext)
    - ✅ Situacion Acad → codificación ordinal (0-3)
    - ✅ Cat_ClaseMax → ccmax_* (50 categorías)
    - ✅ Cat_ClaseMin → ccmin_* (50 categorías)
    - ✅ Tipo Admisión → ta_*
    
    **Resultado**: Base codificada lista para predicción
    """)
    
    # Verificar si existe Libro1.xlsx
    import os
    mapa_categorias_path = "Libro1.xlsx"
    archivo_categorias_existe = os.path.exists(mapa_categorias_path)
    
    if not archivo_categorias_existe:
        st.error("❌ Archivo 'Libro1.xlsx' no encontrado en la raíz del proyecto")
        st.info("💡 Este archivo es necesario para mapear las materias a categorías")
        st.stop()
    
    st.success(f"✅ Archivo de categorías encontrado: {mapa_categorias_path}")
    
    # Tabs para diferentes opciones
    tab_upload, tab_session = st.tabs(["📤 Subir Base Limpia", "💾 Usar desde Sesión"])
    
    with tab_upload:
        st.subheader("📤 Subir Base Limpia")
        st.info("💡 Sube el archivo Excel o CSV generado en la pestaña de Limpieza")
        
        archivo = st.file_uploader(
            "Selecciona la base limpia",
            type=['xlsx', 'xls', 'csv'],
            help="Debe ser el resultado del proceso de limpieza"
        )
        
        if archivo is not None:
            try:
                with st.spinner("📂 Cargando archivo..."):
                    # Leer archivo
                    if archivo.name.endswith('.csv'):
                        data_limpia = pd.read_csv(archivo)
                    else:
                        data_limpia = pd.read_excel(archivo)
                    
                    st.success("✅ Archivo cargado correctamente")
                    
                    # Mostrar información
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("📝 Registros", len(data_limpia))
                    with col2:
                        st.metric("📊 Columnas", len(data_limpia.columns))
                    
                    # Guardar en session_state
                    st.session_state['data_limpia_upload'] = data_limpia
                    st.session_state['source'] = 'upload'
            
            except Exception as e:
                st.error(f"❌ Error al leer el archivo: {str(e)}")
                st.exception(e)
    
    with tab_session:
        st.subheader("💾 Usar Base Limpia de la Sesión")
        
        if 'data_limpia' in st.session_state:
            data_limpia_session = st.session_state['data_limpia']
            st.success("✅ Base limpia encontrada en la sesión actual")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("📝 Registros", len(data_limpia_session))
            with col2:
                st.metric("📊 Columnas", len(data_limpia_session.columns))
            
            if st.button("✅ Usar esta base", type="primary"):
                st.session_state['data_limpia_upload'] = data_limpia_session
                st.session_state['source'] = 'session'
                st.success("✅ Base cargada desde la sesión")
                st.rerun()
        else:
            st.warning("⚠️ No hay base limpia en la sesión actual")
            st.info("💡 Procesa primero la limpieza en la pestaña 🧹 Limpieza")
    
    # Procesar si hay datos cargados
    if 'data_limpia_upload' in st.session_state:
        st.markdown("---")
        
        data_limpia = st.session_state['data_limpia_upload']
        source = st.session_state.get('source', 'unknown')
        
        if source == 'upload':
            st.info("📤 Usando base subida manualmente")
        elif source == 'session':
            st.info("💾 Usando base de la sesión de limpieza")
        
        # Mostrar vista previa
        with st.expander("👁️ Vista previa de la base limpia", expanded=False):
            st.dataframe(data_limpia.head(50), use_container_width=True, height=300)
        
        # Botón para procesar
        if st.button("🚀 PROCESAR ENCODING COMPLETO", type="primary", use_container_width=True):
            with st.spinner("⏳ Procesando encoding (11 fases)..."):
                # Capturar logs
                import sys
                from io import StringIO
                
                old_stdout = sys.stdout
                sys.stdout = buffer = StringIO()
                
                try:
                    # Procesar encoding
                    data_encoded = procesar_encoding_completo(
                        data_limpia, 
                        mapa_categorias_path
                    )
                    
                    # Obtener logs
                    logs = buffer.getvalue()
                    sys.stdout = old_stdout
                    
                    # Guardar en session_state
                    st.session_state['data_encoded'] = data_encoded
                    st.session_state['logs_encoding'] = logs
                    
                except Exception as e:
                    sys.stdout = old_stdout
                    st.error(f"❌ Error durante el encoding: {str(e)}")
                    st.exception(e)
                    return
            
            st.success("✅ Encoding completado!")
            st.balloons()
            
            # Mostrar logs
            with st.expander("📋 Ver logs de procesamiento", expanded=True):
                st.code(logs, language="text")
    
    # Mostrar resultados si existen
    if 'data_encoded' in st.session_state:
        st.markdown("---")
        st.subheader("📊 Resultados del Encoding")
        
        data_encoded = st.session_state['data_encoded']
        
        # Métricas principales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📝 Registros", len(data_encoded))
        
        with col2:
            st.metric("📊 Columnas Totales", len(data_encoded.columns))
        
        with col3:
            # Contar dummies
            dummies_count = sum(1 for c in data_encoded.columns if c.startswith(('p_', 's_', 'cd_', 'dn_', 'ccmax_', 'ccmin_', 'ta_')))
            st.metric("🎨 Variables Dummy", dummies_count)
        
        with col4:
            # Verificar Estado (Dropout)
            if 'Estado (Dropout)' in data_encoded.columns:
                dropout_count = (data_encoded['Estado (Dropout)'] == 1).sum()
                st.metric("⚠️ Dropout = 1", dropout_count)
            else:
                st.metric("⚠️ Dropout", "N/A")
        
        # Tabs para explorar
        tab1, tab2, tab3, tab4 = st.tabs(["👁️ Vista Previa", "📋 Variables Dummy", "📊 Estadísticas", "⚠️ Verificaciones"])
        
        with tab1:
            st.write("**Primeras 100 filas:**")
            st.dataframe(
                data_encoded.head(100),
                use_container_width=True,
                height=400
            )
        
        with tab2:
            st.write("### Variables Dummy Generadas")
            
            # Agrupar por prefijo
            prefijos = {
                'p_': 'Programas',
                's_': 'Siglas Prog',
                'cd_': 'Ciudades',
                'dn_': 'Departamentos',
                'ccmax_': 'Cat. Materia Máxima',
                'ccmin_': 'Cat. Materia Mínima',
                'ta_': 'Tipo Admisión'
            }
            
            for prefijo, nombre in prefijos.items():
                cols = [c for c in data_encoded.columns if c.startswith(prefijo)]
                if cols:
                    with st.expander(f"**{nombre}** ({len(cols)} variables)"):
                        # Mostrar en columnas
                        n_cols = 3
                        for i in range(0, len(cols), n_cols):
                            cols_chunk = cols[i:i+n_cols]
                            cols_display = st.columns(n_cols)
                            for j, col in enumerate(cols_chunk):
                                with cols_display[j]:
                                    # Nombre sin prefijo
                                    nombre_corto = col.replace(prefijo, '')
                                    # Contar 1s
                                    count = (data_encoded[col] == 1).sum() if col in data_encoded.columns else 0
                                    st.write(f"• `{nombre_corto}` ({count})")
        
        with tab3:
            st.write("**Información del DataFrame:**")
            
            buffer = io.StringIO()
            data_encoded.info(buf=buffer)
            info_str = buffer.getvalue()
            st.text(info_str)
            
            st.write("**Estadísticas de variables numéricas:**")
            st.dataframe(data_encoded.describe(), use_container_width=True)
        
        with tab4:
            st.write("### Verificaciones de Calidad")
            
            # 1. Verificar Estado (Dropout)
            st.write("**1. ¿Se creó Estado (Dropout)?**")
            if 'Estado (Dropout)' in data_encoded.columns:
                distribucion = data_encoded['Estado (Dropout)'].value_counts()
                st.success("✅ Estado (Dropout) creado correctamente")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Activo (0)", distribucion.get(0, 0))
                with col2:
                    st.metric("En Dropout (1)", distribucion.get(1, 0))
            else:
                st.error("❌ Estado (Dropout) NO fue creado")
            
            # 2. Verificar Situacion Acad Cod
            st.write("**2. ¿Se codificó Situacion Acad?**")
            if 'Situacion Acad Cod' in data_encoded.columns:
                valores = sorted(data_encoded['Situacion Acad Cod'].unique())
                st.success(f"✅ Situacion Acad Cod creada con valores: {valores}")
                distribucion = data_encoded['Situacion Acad Cod'].value_counts().sort_index()
                st.write(distribucion.to_dict())
            else:
                st.error("❌ Situacion Acad Cod NO fue creada")
            
            # 3. Verificar categorías de materias
            st.write("**3. ¿Se crearon categorías de materias?**")
            ccmax_cols = [c for c in data_encoded.columns if c.startswith('ccmax_')]
            ccmin_cols = [c for c in data_encoded.columns if c.startswith('ccmin_')]
            
            if ccmax_cols and ccmin_cols:
                st.success(f"✅ Categorías creadas: {len(ccmax_cols)} ccmax + {len(ccmin_cols)} ccmin")
            else:
                st.warning(f"⚠️ Categorías encontradas: {len(ccmax_cols)} ccmax, {len(ccmin_cols)} ccmin")
            
            # 4. Verificar transformaciones binarias
            st.write("**4. Transformaciones binarias:**")
            
            binarias_esperadas = {
                'Benef. Beca': [0, 1],
                'Sexo': [0, 1],
                'internacional': [0, 1]
            }
            
            for col, valores_esperados in binarias_esperadas.items():
                if col in data_encoded.columns:
                    valores_reales = sorted(data_encoded[col].dropna().unique())
                    if valores_reales == valores_esperados:
                        st.success(f"✅ {col}: {valores_reales}")
                    else:
                        st.warning(f"⚠️ {col}: {valores_reales} (esperado: {valores_esperados})")
                else:
                    st.error(f"❌ {col}: NO encontrada")
            
            # 5. Verificar que se eliminaron columnas originales
            st.write("**5. ¿Se eliminaron columnas originales?**")
            
            cols_que_no_deben_existir = [
                'Programa', 'Siglas Prog', 'Ciudad (Dirección)', 
                'Dpto Nacimiento', 'País Nacimiento', 'Estado',
                'Tipo Admisión', 'Cat_ClaseMax', 'Cat_ClaseMin',
                'Clase_Max_Ciclo', 'Clase_Min_Ciclo',
                'ID_Max_Ciclo', 'ID_Min_Ciclo'
            ]
            
            cols_encontradas = [c for c in cols_que_no_deben_existir if c in data_encoded.columns]
            
            if not cols_encontradas:
                st.success("✅ Todas las columnas originales fueron eliminadas correctamente")
            else:
                st.warning(f"⚠️ Aún existen {len(cols_encontradas)} columnas originales:")
                st.write(cols_encontradas)
            
            # 6. Verificar tipos de datos
            st.write("**6. Tipos de datos:**")
            tipos = data_encoded.dtypes.value_counts()
            st.write(tipos.to_dict())
            
            # Verificar que no haya object (excepto algunas permitidas)
            cols_object = data_encoded.select_dtypes(include=['object']).columns.tolist()
            cols_object_permitidas = ['ID', 'Situacion Acad']  # Columnas que sí pueden ser object
            cols_object_problematicas = [c for c in cols_object if c not in cols_object_permitidas]
            
            if cols_object_problematicas:
                st.warning(f"⚠️ {len(cols_object_problematicas)} columnas tipo 'object' (deberían ser numéricas):")
                st.write(cols_object_problematicas[:10])
            else:
                st.success("✅ No hay columnas tipo 'object' problemáticas")
        
        # Botones de descarga
        st.markdown("---")
        st.subheader("📥 Descargar Base Codificada")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Descargar como Excel
            buffer_excel = io.BytesIO()
            with pd.ExcelWriter(buffer_excel, engine='openpyxl') as writer:
                data_encoded.to_excel(writer, index=False, sheet_name='Base Codificada')
            
            st.download_button(
                label="📊 Descargar Excel",
                data=buffer_excel.getvalue(),
                file_name="base_codificada.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        with col2:
            # Descargar como CSV
            csv = data_encoded.to_csv(index=False)
            st.download_button(
                label="📄 Descargar CSV",
                data=csv,
                file_name="base_codificada.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col3:
            # Descargar como Pickle (más rápido para cargar después)
            buffer_pickle = io.BytesIO()
            data_encoded.to_pickle(buffer_pickle)
            st.download_button(
                label="🗜️ Descargar Pickle",
                data=buffer_pickle.getvalue(),
                file_name="base_codificada.pkl",
                mime="application/octet-stream",
                use_container_width=True
            )
        
        # Información adicional
        st.info("""
        **💡 Formatos disponibles:**
        - **Excel (.xlsx)**: Fácil de abrir y visualizar
        - **CSV (.csv)**: Ligero y compatible con cualquier herramienta
        - **Pickle (.pkl)**: Más rápido para cargar en Python, conserva tipos de datos exactos
        """)


# =============================================================================
# INTEGRACIÓN CON APP PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    st.set_page_config(
        page_title="Encoding de Datos",
        page_icon="🎨",
        layout="wide"
    )
    
    st.title("🎨 Procesador de Encoding - 11 Fases")
    
    seccion_encoding()
