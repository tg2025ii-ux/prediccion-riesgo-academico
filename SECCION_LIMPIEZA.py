"""
Componente Streamlit para probar PASO 1: LIMPIEZA
Permite descargar la base limpia
"""

import streamlit as st
import pandas as pd
import io
from data_processor_limpieza import DataProcessorLimpieza

def seccion_limpieza():
    """
    Sección de Streamlit para probar la limpieza (Pasos 1-13)
    """
    st.header("🧹 PASO 1: LIMPIEZA Y PREPARACIÓN")
    st.markdown("""
    Esta sección procesa las 4 bases y genera una base limpia aplicando:
    - ✅ Renombres de columnas
    - ✅ Eliminación de IDs fallecidos
    - ✅ Filtros de ciclos y créditos
    - ✅ Transformación de Mult Programa
    - ✅ Merge de las 4 bases
    - ✅ Resolución de duplicados
    - ✅ **Eliminación de Acción y Motivo**
    - ✅ Cálculo de Siglas Prog (moda)
    - ✅ Relleno de datos
    """)
    
    # Subir archivo
    st.subheader("📤 Subir archivo Excel")
    archivo = st.file_uploader(
        "Selecciona tu archivo Excel con 4 hojas (NOTAS, PER, PROM, ADM)",
        type=['xlsx', 'xls'],
        help="El archivo debe contener las 4 hojas: NOTAS, PER, PROM, ADM"
    )
    
    if archivo is not None:
        try:
            with st.spinner("🔄 Procesando archivo..."):
                # Leer las 4 hojas
                notas = pd.read_excel(archivo, sheet_name='NOTAS')
                per = pd.read_excel(archivo, sheet_name='PER')
                prom = pd.read_excel(archivo, sheet_name='PROM')
                adm = pd.read_excel(archivo, sheet_name='ADM')
                
                st.success("✅ Archivo cargado correctamente")
                
                # Mostrar información de las bases
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("NOTAS", f"{len(notas)} registros")
                with col2:
                    st.metric("PER", f"{len(per)} registros")
                with col3:
                    st.metric("PROM", f"{len(prom)} registros")
                with col4:
                    st.metric("ADM", f"{len(adm)} registros")
            
            # Botón para procesar
            if st.button("🚀 PROCESAR LIMPIEZA", type="primary"):
                with st.spinner("⏳ Procesando limpieza (Pasos 1-13)..."):
                    # Crear procesador
                    procesador = DataProcessorLimpieza()
                    
                    # Capturar logs en un expander
                    with st.expander("📋 Ver logs de procesamiento", expanded=True):
                        # Redirigir prints a un contenedor
                        import sys
                        from io import StringIO
                        
                        old_stdout = sys.stdout
                        sys.stdout = buffer = StringIO()
                        
                        try:
                            # Procesar
                            data_limpia = procesador.procesar_dataframes(notas, per, prom, adm)
                            
                            # Obtener logs
                            logs = buffer.getvalue()
                            sys.stdout = old_stdout
                            
                            # Mostrar logs
                            st.code(logs, language="text")
                            
                            # Guardar en session_state
                            st.session_state['data_limpia'] = data_limpia
                            st.session_state['logs_limpieza'] = logs
                            
                        except Exception as e:
                            sys.stdout = old_stdout
                            st.error(f"❌ Error durante el procesamiento: {str(e)}")
                            st.exception(e)
                            return
                
                st.success("✅ Limpieza completada!")
                st.balloons()
        
        except Exception as e:
            st.error(f"❌ Error al leer el archivo: {str(e)}")
            st.exception(e)
    
    # Mostrar resultados si existen
    if 'data_limpia' in st.session_state:
        st.markdown("---")
        st.subheader("📊 Resultados de la Limpieza")
        
        data_limpia = st.session_state['data_limpia']
        
        # Métricas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📝 Registros", len(data_limpia))
        with col2:
            st.metric("📊 Columnas", len(data_limpia.columns))
        with col3:
            # Verificar que NO existan Acción y Motivo
            accion_cols = [c for c in data_limpia.columns if 'Acción' in c or 'Accion' in c]
            motivo_cols = [c for c in data_limpia.columns if 'Motivo' in c]
            total_am = len(accion_cols) + len(motivo_cols)
            
            if total_am == 0:
                st.metric("✅ Acción/Motivo", "ELIMINADAS", delta="0 columnas")
            else:
                st.metric("⚠️ Acción/Motivo", f"{total_am} columnas", delta_color="off")
        
        # Tabs para explorar
        tab1, tab2, tab3, tab4 = st.tabs(["👁️ Vista Previa", "📋 Columnas", "📊 Estadísticas", "⚠️ Verificaciones"])
        
        with tab1:
            st.dataframe(
                data_limpia.head(100),
                use_container_width=True,
                height=400
            )
        
        with tab2:
            st.write(f"**Total de columnas:** {len(data_limpia.columns)}")
            
            # Agrupar columnas por tipo
            columnas_por_tipo = {}
            for col in data_limpia.columns:
                tipo = str(data_limpia[col].dtype)
                if tipo not in columnas_por_tipo:
                    columnas_por_tipo[tipo] = []
                columnas_por_tipo[tipo].append(col)
            
            for tipo, cols in columnas_por_tipo.items():
                with st.expander(f"**{tipo}** ({len(cols)} columnas)"):
                    st.write(", ".join(sorted(cols)))
        
        with tab3:
            st.write("**Información del DataFrame:**")
            
            buffer = io.StringIO()
            data_limpia.info(buf=buffer)
            info_str = buffer.getvalue()
            st.text(info_str)
            
            st.write("**Estadísticas de columnas numéricas:**")
            st.dataframe(data_limpia.describe(), use_container_width=True)
        
        with tab4:
            st.write("### Verificaciones Importantes")
            
            # 1. Verificar que NO existan Acción y Motivo
            st.write("**1. ¿Se eliminaron Acción y Motivo?**")
            accion_cols = [c for c in data_limpia.columns if 'Acción' in c or 'Accion' in c]
            motivo_cols = [c for c in data_limpia.columns if 'Motivo' in c]
            
            if not accion_cols and not motivo_cols:
                st.success("✅ Acción y Motivo fueron eliminadas correctamente")
            else:
                st.error(f"❌ Aún existen columnas:")
                if accion_cols:
                    st.write(f"   - Acción: {accion_cols}")
                if motivo_cols:
                    st.write(f"   - Motivo: {motivo_cols}")
            
            # 2. Verificar que exista Siglas Prog
            st.write("**2. ¿Se creó Siglas Prog?**")
            if 'Siglas Prog' in data_limpia.columns:
                st.success(f"✅ Siglas Prog existe con {data_limpia['Siglas Prog'].nunique()} valores únicos")
                st.write(f"   Valores: {data_limpia['Siglas Prog'].value_counts().head(10).to_dict()}")
            else:
                st.error("❌ Siglas Prog NO fue creada")
            
            # 3. Verificar columnas con sufijos
            st.write("**3. ¿Existen columnas con sufijos?**")
            sufijos = ['_per', '_prom', '_adm', '_ppn', '_pprom', '_notas']
            cols_sufijos = [c for c in data_limpia.columns if any(c.endswith(s) for s in sufijos)]
            
            if cols_sufijos:
                st.warning(f"⚠️ Aún existen {len(cols_sufijos)} columnas con sufijos:")
                st.write(cols_sufijos)
            else:
                st.success("✅ No hay columnas con sufijos duplicados")
            
            # 4. Verificar valores nulos críticos
            st.write("**4. Valores nulos en columnas críticas:**")
            cols_criticas = ['ID', 'Mult Programa', 'Programa', 'Ciclo', 'Siglas Prog']
            cols_criticas = [c for c in cols_criticas if c in data_limpia.columns]
            
            nulos_criticos = data_limpia[cols_criticas].isnull().sum()
            nulos_criticos = nulos_criticos[nulos_criticos > 0]
            
            if len(nulos_criticos) == 0:
                st.success("✅ No hay nulos en columnas críticas")
            else:
                st.warning("⚠️ Nulos encontrados:")
                st.write(nulos_criticos.to_dict())
        
        # Botones de descarga
        st.markdown("---")
        st.subheader("📥 Descargar Base Limpia")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Descargar como Excel
            buffer_excel = io.BytesIO()
            with pd.ExcelWriter(buffer_excel, engine='openpyxl') as writer:
                data_limpia.to_excel(writer, index=False, sheet_name='Base Limpia')
            
            st.download_button(
                label="📊 Descargar Excel",
                data=buffer_excel.getvalue(),
                file_name="base_limpia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col2:
            # Descargar como CSV
            csv = data_limpia.to_csv(index=False)
            st.download_button(
                label="📄 Descargar CSV",
                data=csv,
                file_name="base_limpia.csv",
                mime="text/csv"
            )


# =============================================================================
# INTEGRACIÓN CON APP PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    st.set_page_config(
        page_title="Limpieza de Datos",
        page_icon="🧹",
        layout="wide"
    )
    
    st.title("🧹 Procesador de Limpieza - Pasos 1-13")
    
    seccion_limpieza()
