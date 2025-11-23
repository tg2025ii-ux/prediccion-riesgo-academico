"""
Componente Streamlit para AJUSTES FINALES
Permite cargar base codificada y descargar base lista para predicción
"""

import streamlit as st
import pandas as pd
import io
from data_processor_ajustes import DataProcessorAjustes, procesar_ajustes_completo

def seccion_ajustes():
    """
    Sección de Streamlit para ajustes finales (dropout corrida)
    """
    st.header("🔧 AJUSTES FINALES (11 FASES)")
    st.markdown("""
    Esta sección aplica los ajustes finales a la base codificada:
    
    **FASE 1: Eliminar Programas Finalizados**
    - ✅ Filtrar Estado = "Programa Finalizado"
    
    **FASE 2-3: Calcular Dropout Corrida**
    - ✅ Si último ciclo tiene Dropout=1, poner 0 a anteriores
    - ✅ Ajustar dropout en último ciclo
    
    **FASE 4: Detectar Pausas Largas**
    - ✅ Pausas ≥3 semestres → marcar como deserción
    
    **FASE 5: Crear Estado_next**
    - ✅ Variable de predicción (deserción en próximo ciclo)
    - ✅ Dos últimos ciclos → NaN
    - ✅ Si último Dropout=1 → marcar dos ciclos antes
    
    **FASE 6: Validar con PER**
    - ✅ Verificar continuidad con base PER original
    
    **FASE 7-8: Limpiar y Convertir**
    - ✅ Renombrar columnas duplicadas
    - ✅ Eliminar columnas innecesarias
    - ✅ Convertir tipos de datos (bool→int8)
    
    **FASE 9: Filtrar Registros Válidos**
    - ✅ Solo registros con Estado_next válido
    - ✅ Renombrar Estado_next → desercion
    
    **FASE 10: Crear Rango Edad**
    - ✅ Variable categórica de edad (0-3)
    
    **FASE 11: Aplicar Columnas del Modelo**
    - ✅ Orden y selección de columnas exactas del modelo
    
    **Resultado**: Base final lista para predicción con modelo XGBoost
    """)
    
    # Verificar archivos necesarios
    import os
    libro1_existe = os.path.exists("Libro1.xlsx")
    
    if libro1_existe:
        st.success("✅ Archivo 'Libro1.xlsx' encontrado")
    else:
        st.warning("⚠️ Archivo 'Libro1.xlsx' no encontrado (opcional para columnas del modelo)")
    
    # Tabs para diferentes opciones
    tab_upload, tab_session = st.tabs(["📤 Subir Base Codificada", "💾 Usar desde Sesión"])
    
    with tab_upload:
        st.subheader("📤 Subir Base Codificada")
        st.info("💡 Sube el archivo Excel o CSV generado en la pestaña de Encoding")
        
        col1, col2 = st.columns(2)
        
        with col1:
            archivo_encoded = st.file_uploader(
                "Selecciona la base codificada",
                type=['xlsx', 'xls', 'csv', 'pkl'],
                help="Debe ser el resultado del proceso de encoding",
                key="upload_encoded"
            )
        
        with col2:
            archivo_per = st.file_uploader(
                "Selecciona PER original (opcional)",
                type=['xlsx', 'xls', 'csv'],
                help="Para validar continuidad de estudiantes",
                key="upload_per"
            )
        
        if archivo_encoded is not None:
            try:
                with st.spinner("📂 Cargando archivo codificado..."):
                    # Leer archivo
                    if archivo_encoded.name.endswith('.pkl'):
                        data_encoded = pd.read_pickle(archivo_encoded)
                    elif archivo_encoded.name.endswith('.csv'):
                        data_encoded = pd.read_csv(archivo_encoded)
                    else:
                        data_encoded = pd.read_excel(archivo_encoded)
                    
                    st.success("✅ Base codificada cargada correctamente")
                    
                    # Mostrar información
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("📝 Registros", len(data_encoded))
                    with col2:
                        st.metric("📊 Columnas", len(data_encoded.columns))
                    
                    # Guardar en session_state
                    st.session_state['data_encoded_upload'] = data_encoded
                    st.session_state['source_ajustes'] = 'upload'
            
            except Exception as e:
                st.error(f"❌ Error al leer el archivo: {str(e)}")
                st.exception(e)
        
        # Leer PER si se proporciona
        if archivo_per is not None:
            try:
                with st.spinner("📂 Cargando PER original..."):
                    if archivo_per.name.endswith('.csv'):
                        per_original = pd.read_csv(archivo_per)
                    else:
                        per_original = pd.read_excel(archivo_per, sheet_name='PER')
                    
                    st.success("✅ PER original cargado")
                    st.session_state['per_original_upload'] = per_original
                    
                    st.metric("📝 Registros PER", len(per_original))
            
            except Exception as e:
                st.warning(f"⚠️ Error al leer PER: {str(e)}")
    
    with tab_session:
        st.subheader("💾 Usar Base Codificada de la Sesión")
        
        if 'data_encoded' in st.session_state:
            data_encoded_session = st.session_state['data_encoded']
            st.success("✅ Base codificada encontrada en la sesión actual")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("📝 Registros", len(data_encoded_session))
            with col2:
                st.metric("📊 Columnas", len(data_encoded_session.columns))
            
            if st.button("✅ Usar esta base", type="primary"):
                st.session_state['data_encoded_upload'] = data_encoded_session
                st.session_state['source_ajustes'] = 'session'
                st.success("✅ Base cargada desde la sesión")
                st.rerun()
        else:
            st.warning("⚠️ No hay base codificada en la sesión actual")
            st.info("💡 Procesa primero el encoding en la pestaña 🎨 Encoding")
    
    # Procesar si hay datos cargados
    if 'data_encoded_upload' in st.session_state:
        st.markdown("---")
        
        data_encoded = st.session_state['data_encoded_upload']
        source = st.session_state.get('source_ajustes', 'unknown')
        
        if source == 'upload':
            st.info("📤 Usando base subida manualmente")
        elif source == 'session':
            st.info("💾 Usando base de la sesión de encoding")
        
        # Opciones avanzadas
        with st.expander("⚙️ Opciones Avanzadas", expanded=False):
            usar_per = st.checkbox(
                "Usar validación con PER original",
                value='per_original_upload' in st.session_state,
                help="Valida continuidad de estudiantes con base PER"
            )
            
            usar_columnas_modelo = st.checkbox(
                "Aplicar columnas del modelo (Libro1.xlsx)",
                value=libro1_existe,
                help="Ordena columnas según el modelo entrenado"
            )
        
        # Mostrar vista previa
        with st.expander("👁️ Vista previa de la base codificada", expanded=False):
            st.dataframe(data_encoded.head(50), use_container_width=True, height=300)
        
        # Botón para procesar
        if st.button("🚀 PROCESAR AJUSTES FINALES", type="primary", use_container_width=True):
            with st.spinner("⏳ Procesando ajustes finales (11 fases)..."):
                # Capturar logs
                import sys
                from io import StringIO
                
                old_stdout = sys.stdout
                sys.stdout = buffer = StringIO()
                
                try:
                    # Preparar argumentos
                    per_original = st.session_state.get('per_original_upload', None) if usar_per else None
                    columnas_modelo_path = "Libro1.xlsx" if usar_columnas_modelo and libro1_existe else None
                    
                    # Procesar ajustes
                    data_final = procesar_ajustes_completo(
                        data_encoded, 
                        per_original,
                        columnas_modelo_path
                    )
                    
                    # Obtener logs
                    logs = buffer.getvalue()
                    sys.stdout = old_stdout
                    
                    # Guardar en session_state
                    st.session_state['data_final'] = data_final
                    st.session_state['logs_ajustes'] = logs
                    
                except Exception as e:
                    sys.stdout = old_stdout
                    st.error(f"❌ Error durante los ajustes: {str(e)}")
                    st.exception(e)
                    return
            
            st.success("✅ Ajustes completados!")
            st.balloons()
            
            # Mostrar logs
            with st.expander("📋 Ver logs de procesamiento", expanded=True):
                st.code(logs, language="text")
    
    # Mostrar resultados si existen
    if 'data_final' in st.session_state:
        st.markdown("---")
        st.subheader("📊 Resultados de los Ajustes")
        
        data_final = st.session_state['data_final']
        
        # ⚠️ VERIFICAR Y ELIMINAR DUPLICADOS
        if data_final.columns.duplicated().any():
            st.warning("⚠️ Detectadas columnas duplicadas. Eliminando automáticamente...")
            duplicados = data_final.columns[data_final.columns.duplicated()].tolist()
            st.write(f"Duplicados encontrados: {duplicados[:10]}")
            data_final = data_final.loc[:, ~data_final.columns.duplicated()]
            st.session_state['data_final'] = data_final  # Actualizar
            st.success(f"✅ Duplicados eliminados. Columnas actuales: {len(data_final.columns)}")
        
        # Métricas principales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📝 Registros", len(data_final))
        
        with col2:
            st.metric("📊 Columnas", len(data_final.columns))
        
        with col3:
            # Verificar desercion
            if 'desercion' in data_final.columns:
                desercion_count = (data_final['desercion'] == 1).sum()
                st.metric("⚠️ Deserción = 1", desercion_count)
            else:
                st.metric("⚠️ Deserción", "N/A")
        
        with col4:
            # Memoria del DataFrame
            memoria_mb = data_final.memory_usage(deep=True).sum() / 1024**2
            st.metric("💾 Tamaño", f"{memoria_mb:.1f} MB")
        
        # Tabs para explorar
        tab1, tab2, tab3, tab4 = st.tabs(["👁️ Vista Previa", "📋 Columnas", "📊 Estadísticas", "⚠️ Verificaciones"])
        
        with tab1:
            st.write("**Primeras 100 filas:**")
            st.dataframe(
                data_final.head(100),
                use_container_width=True,
                height=400
            )
        
        with tab2:
            st.write("### Lista de Columnas")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Mostrar todas las columnas
                columnas_df = pd.DataFrame({
                    'Columna': data_final.columns,
                    'Tipo': data_final.dtypes.astype(str),
                    'No Nulos': data_final.count().values,
                    'Nulos': data_final.isnull().sum().values
                })
                st.dataframe(columnas_df, use_container_width=True, height=600)
            
            with col2:
                # Resumen por tipo
                st.write("**Tipos de datos:**")
                tipos = data_final.dtypes.value_counts()
                for tipo, count in tipos.items():
                    st.write(f"• `{tipo}`: {count} columnas")
                
                # Prefijos
                st.write("\n**Prefijos:**")
                prefijos = {}
                for col in data_final.columns:
                    if '_' in col:
                        prefijo = col.split('_')[0]
                        prefijos[prefijo] = prefijos.get(prefijo, 0) + 1
                
                for prefijo, count in sorted(prefijos.items()):
                    st.write(f"• `{prefijo}_*`: {count} columnas")
        
        with tab3:
            st.write("**Información del DataFrame:**")
            
            buffer = io.StringIO()
            data_final.info(buf=buffer)
            info_str = buffer.getvalue()
            st.text(info_str)
            
            st.write("**Estadísticas de variables numéricas:**")
            st.dataframe(data_final.describe(), use_container_width=True)
        
        with tab4:
            st.write("### Verificaciones de Calidad")
            
            # 1. Verificar desercion
            st.write("**1. ¿Se creó la variable 'desercion'?**")
            if 'desercion' in data_final.columns:
                distribucion = data_final['desercion'].value_counts()
                st.success("✅ Variable 'desercion' creada correctamente")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("No Deserción (0)", distribucion.get(0, 0))
                with col2:
                    st.metric("Deserción (1)", distribucion.get(1, 0))
                
                # Porcentaje
                if len(data_final) > 0:
                    porcentaje = (distribucion.get(1, 0) / len(data_final)) * 100
                    st.write(f"   📊 Tasa de deserción: **{porcentaje:.2f}%**")
            else:
                st.error("❌ Variable 'desercion' NO fue creada")
            
            # 2. Verificar rango_edad
            st.write("**2. ¿Se creó 'rango_edad'?**")
            if 'rango_edad' in data_final.columns:
                valores = sorted(data_final['rango_edad'].unique())
                st.success(f"✅ rango_edad creada con valores: {valores}")
                distribucion = data_final['rango_edad'].value_counts().sort_index()
                st.write(distribucion.to_dict())
            else:
                st.warning("⚠️ rango_edad NO fue creada")
            
            # 3. Verificar que no hay NaN en desercion
            st.write("**3. ¿Hay valores NaN en 'desercion'?**")
            if 'desercion' in data_final.columns:
                nulos = data_final['desercion'].isna().sum()
                if nulos == 0:
                    st.success("✅ No hay valores NaN en desercion")
                else:
                    st.error(f"❌ Hay {nulos} valores NaN en desercion")
            
            # 4. Verificar tipos de datos
            st.write("**4. Tipos de datos:**")
            tipos = data_final.dtypes.value_counts()
            st.write(tipos.to_dict())
            
            # Verificar que no hay object
            cols_object = data_final.select_dtypes(include=['object']).columns.tolist()
            if cols_object:
                st.warning(f"⚠️ {len(cols_object)} columnas tipo 'object':")
                st.write(cols_object[:10])
            else:
                st.success("✅ No hay columnas tipo 'object'")
            
            # 5. Verificar que se eliminaron columnas innecesarias
            st.write("**5. Columnas eliminadas correctamente:**")
            
            cols_que_no_deben_existir = [
                'ID', 'Estado', 'Estado (Dropout)', 'Programa',
                'Situacion Acad', 'Estado_next'
            ]
            
            cols_encontradas = [c for c in cols_que_no_deben_existir if c in data_final.columns]
            
            if not cols_encontradas:
                st.success("✅ Todas las columnas innecesarias fueron eliminadas")
            else:
                st.warning(f"⚠️ Aún existen {len(cols_encontradas)} columnas:")
                st.write(cols_encontradas)
            
            # 6. Distribución de columnas dummy
            st.write("**6. Variables Dummy:**")
            
            prefijos_dummy = ['p_', 's_', 'cd_', 'dn_', 'ccmax_', 'ccmin_', 'ta_']
            total_dummies = 0
            
            for prefijo in prefijos_dummy:
                cols = [c for c in data_final.columns if c.startswith(prefijo)]
                if cols:
                    total_dummies += len(cols)
                    st.write(f"   • `{prefijo}*`: {len(cols)} columnas")
            
            st.write(f"   **Total dummies**: {total_dummies}")
            
            # 7. Verificar balance de clases
            st.write("**7. Balance de Clases (desercion):**")
            if 'desercion' in data_final.columns:
                distribucion = data_final['desercion'].value_counts()
                total = len(data_final)
                
                for valor in [0, 1]:
                    count = distribucion.get(valor, 0)
                    porcentaje = (count / total) * 100 if total > 0 else 0
                    st.write(f"   • Clase {valor}: {count:,} ({porcentaje:.2f}%)")
                
                # Advertencia si está muy desbalanceado
                if total > 0:
                    min_class = min(distribucion.get(0, 0), distribucion.get(1, 0))
                    max_class = max(distribucion.get(0, 0), distribucion.get(1, 0))
                    ratio = max_class / min_class if min_class > 0 else float('inf')
                    
                    if ratio > 10:
                        st.warning(f"⚠️ Clases muy desbalanceadas (ratio: {ratio:.1f}:1)")
                    else:
                        st.success(f"✅ Balance aceptable (ratio: {ratio:.1f}:1)")
        
        # Botones de descarga
        st.markdown("---")
        st.subheader("📥 Descargar Base Final")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Descargar como Excel
            buffer_excel = io.BytesIO()
            with pd.ExcelWriter(buffer_excel, engine='openpyxl') as writer:
                data_final.to_excel(writer, index=False, sheet_name='Base Final')
            
            st.download_button(
                label="📊 Descargar Excel",
                data=buffer_excel.getvalue(),
                file_name="base_final_prediccion.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        with col2:
            # Descargar como CSV
            csv = data_final.to_csv(index=False)
            st.download_button(
                label="📄 Descargar CSV",
                data=csv,
                file_name="base_final_prediccion.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col3:
            # Descargar como Pickle
            buffer_pickle = io.BytesIO()
            data_final.to_pickle(buffer_pickle)
            st.download_button(
                label="🗜️ Descargar Pickle",
                data=buffer_pickle.getvalue(),
                file_name="base_final_prediccion.pkl",
                mime="application/octet-stream",
                use_container_width=True
            )
        
        # Información adicional
        st.success("""
        ✅ **Base lista para predicción con modelo XGBoost**
        
        Esta base incluye:
        - Variable objetivo: `desercion` (0/1)
        - Todas las variables dummy codificadas
        - Variables numéricas normalizadas
        - Sin valores nulos en columnas críticas
        - Formato optimizado para el modelo
        """)


# =============================================================================
# INTEGRACIÓN CON APP PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    st.set_page_config(
        page_title="Ajustes Finales",
        page_icon="🔧",
        layout="wide"
    )
    
    st.title("🔧 Procesador de Ajustes Finales - 11 Fases")
    
    seccion_ajustes()
