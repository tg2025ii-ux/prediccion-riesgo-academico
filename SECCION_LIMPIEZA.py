"""
Componente Streamlit para probar LIMPIEZA COMPLETA
Permite descargar la base limpia
"""

import streamlit as st
import pandas as pd
import io
from data_processor_limpieza_COMPLETO import DataProcessorLimpiezaCompleto

def seccion_limpieza():
    """
    Sección de Streamlit para probar la limpieza (Pasos 1-13)
    """
    st.header("🧹 LIMPIEZA COMPLETA (10 FASES)")
    st.markdown("""
    Esta sección procesa las 4 bases y genera una base limpia aplicando:
    
    **FASE 0: Preparación de NOTAS**
    - ✅ Limpieza inicial de NOTAS
    - ✅ Consolidación (estructura base + Dropout)
    - ✅ Métricas de calificaciones (11 variables)
    - ✅ Métricas adicionales (3 variables)
    
    **FASE 1: Filtros Iniciales**
    - ✅ Eliminar ciclos máximos
    - ✅ Eliminar UCollege Javeriano
    - ✅ Filtrar ADM activos
    - ✅ IDs comunes
    
    **FASE 2-10: Transformaciones**
    - ✅ Rellenar Ciclo Admisión
    - ✅ Renombres y eliminación de columnas
    - ✅ Filtros de calidad (fallecidos, ciclos 10/30, créditos)
    - ✅ Merge de las 4 bases
    - ✅ Resolución de duplicados
    - ✅ **Eliminación de Acción y Motivo**
    - ✅ Cálculo de Siglas Prog (moda)
    - ✅ Limpieza geográfica (ciudades, departamentos)
    - ✅ Cálculo de Edad
    
    **Resultado**: Base limpia lista para encoding (dumificación)
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
            if st.button("🚀 PROCESAR LIMPIEZA COMPLETA", type="primary"):
                with st.spinner("⏳ Procesando limpieza completa (10 fases)..."):
                    # Crear procesador
                    procesador = DataProcessorLimpiezaCompleto()
                    
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
            
            # 3. Verificar variable Dropout
            st.write("**3. ¿Existe la variable Dropout?**")
            if 'Dropout' in data_limpia.columns:
                dropout_count = data_limpia['Dropout'].value_counts()
                st.success("✅ Variable Dropout creada correctamente")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Sin deserción (0)", dropout_count.get(0, 0))
                with col2:
                    st.metric("En deserción (1)", dropout_count.get(1, 0))
            else:
                st.error("❌ Variable Dropout NO fue creada")
            
            # 4. Verificar métricas de calificaciones
            st.write("**4. ¿Se calcularon métricas de calificaciones?**")
            metricas_esperadas = [
                'Promedio_Ciclo', 'Des_Estandar_Ciclo', 
                'Min_Ciclo', 'Max_Ciclo', 
                'Clase_Min_Ciclo', 'Clase_Max_Ciclo',
                'Rango_Ponderado_Ciclo'
            ]
            metricas_encontradas = [m for m in metricas_esperadas if m in data_limpia.columns]
            
            if len(metricas_encontradas) == len(metricas_esperadas):
                st.success(f"✅ Todas las métricas de calificaciones fueron calculadas ({len(metricas_encontradas)}/7)")
                
                # Mostrar estadísticas de las métricas
                if 'Promedio_Ciclo' in data_limpia.columns:
                    promedios = data_limpia['Promedio_Ciclo'].dropna()
                    if len(promedios) > 0:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Promedio general", f"{promedios.mean():.2f}")
                        with col2:
                            st.metric("Promedio mínimo", f"{promedios.min():.2f}")
                        with col3:
                            st.metric("Promedio máximo", f"{promedios.max():.2f}")
            else:
                st.warning(f"⚠️ Solo {len(metricas_encontradas)}/7 métricas encontradas")
                st.write(f"   Faltantes: {set(metricas_esperadas) - set(metricas_encontradas)}")
            
            # 5. Verificar métricas adicionales
            st.write("**5. ¿Se calcularon métricas adicionales?**")
            metricas_adicionales = ['Num_Materias_Ciclo', 'Cant_Perdidas', 'Materias_Vistas']
            metricas_adic_encontradas = [m for m in metricas_adicionales if m in data_limpia.columns]
            
            if len(metricas_adic_encontradas) == len(metricas_adicionales):
                st.success(f"✅ Todas las métricas adicionales fueron calculadas ({len(metricas_adic_encontradas)}/3)")
            else:
                st.warning(f"⚠️ Solo {len(metricas_adic_encontradas)}/3 métricas adicionales encontradas")
            
            # 6. Verificar columnas con sufijos
            st.write("**6. ¿Existen columnas con sufijos?**")
            sufijos = ['_per', '_prom', '_adm', '_ppn', '_pprom', '_notas']
            cols_sufijos = [c for c in data_limpia.columns if any(c.endswith(s) for s in sufijos)]
            
            if cols_sufijos:
                st.warning(f"⚠️ Aún existen {len(cols_sufijos)} columnas con sufijos:")
                st.write(cols_sufijos)
            else:
                st.success("✅ No hay columnas con sufijos duplicados")
            
            # 7. Verificar valores nulos críticos
            st.write("**7. Valores nulos en columnas críticas:**")
            cols_criticas = ['ID', 'Mult Programa', 'Programa_Academico_Base', 'Ciclo', 'Siglas Prog', 'Dropout']
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
        page_title="Limpieza Completa de Datos",
        page_icon="🧹",
        layout="wide"
    )
    
    st.title("🧹 Procesador de Limpieza COMPLETO - 10 Fases")
    
    seccion_limpieza()
