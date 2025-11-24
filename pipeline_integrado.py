"""
Pipeline Integrado - Ejecuta los 3 procesadores en secuencia
Limpieza → Encoding → Ajustes → Listo para XGBoost
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional
import streamlit as st

# Importar los 3 procesadores
from data_processor_limpieza_COMPLETO import procesar_limpieza_completa
from data_processor_encoding import procesar_encoding_completo
from data_processor_ajustes import procesar_ajustes_completo


class PipelineIntegrado:
    """
    Pipeline que ejecuta todo el procesamiento en secuencia
    """
    
    def __init__(self, libro1_path: str = "Libro1.xlsx", columnas_path: str = "columnas.csv"):
        """
        Inicializa el pipeline
        
        Args:
            libro1_path: Ruta al archivo Libro1.xlsx (para encoding)
            columnas_path: Ruta al archivo columnas.csv (para ajustes)
        """
        self.libro1_path = libro1_path
        self.columnas_path = columnas_path
        
    def procesar_completo(self, 
                         notas_df: pd.DataFrame,
                         per_df: pd.DataFrame,
                         prom_df: pd.DataFrame,
                         adm_df: pd.DataFrame,
                         progress_callback=None) -> Tuple[pd.DataFrame, dict]:
        """
        Ejecuta el pipeline completo
        
        Args:
            notas_df: DataFrame de NOTAS
            per_df: DataFrame de PER
            prom_df: DataFrame de PROM
            adm_df: DataFrame de ADM
            progress_callback: Función para actualizar progreso (opcional)
            
        Returns:
            Tuple con (data_final, logs)
        """
        logs = {
            "limpieza": "",
            "encoding": "",
            "ajustes": "",
            "errores": []
        }
        
        try:
            # ============================================================
            # PASO 1/4: LIMPIEZA (10 FASES)
            # ============================================================
            if progress_callback:
                progress_callback(0.1, "🧹 Paso 1/4: Procesando limpieza (10 fases)...")
            
            print("\n" + "="*80)
            print("🧹 PASO 1/4: LIMPIEZA")
            print("="*80)
            
            data_limpia = procesar_limpieza_completa(
                notas_df, 
                per_df, 
                prom_df, 
                adm_df
            )
            
            logs["limpieza"] = "✅ Limpieza completada"
            print(f"✅ Limpieza completada: {len(data_limpia)} registros")
            
            if progress_callback:
                progress_callback(0.35, "✅ Limpieza completada")
            
            # ============================================================
            # PASO 2/4: ENCODING (11 FASES)
            # ============================================================
            if progress_callback:
                progress_callback(0.40, "🎨 Paso 2/4: Procesando encoding (11 fases)...")
            
            print("\n" + "="*80)
            print("🎨 PASO 2/4: ENCODING")
            print("="*80)
            
            data_encoded = procesar_encoding_completo(
                data_limpia,
                self.libro1_path
            )
            
            logs["encoding"] = "✅ Encoding completado"
            print(f"✅ Encoding completado: {len(data_encoded.columns)} columnas")
            
            if progress_callback:
                progress_callback(0.65, "✅ Encoding completado")
            
            # ============================================================
            # PASO 3/4: AJUSTES (12 FASES)
            # ============================================================
            if progress_callback:
                progress_callback(0.70, "🔧 Paso 3/4: Aplicando ajustes finales (12 fases)...")
            
            print("\n" + "="*80)
            print("🔧 PASO 3/4: AJUSTES FINALES")
            print("="*80)
            
            data_final = procesar_ajustes_completo(
                data_encoded,
                per_df,  # PER original para validación
                self.columnas_path
            )
            
            logs["ajustes"] = "✅ Ajustes completados"
            print(f"✅ Ajustes completados: {len(data_final)} registros finales")
            
            if progress_callback:
                progress_callback(0.95, "✅ Ajustes completados")
            
            # ============================================================
            # VALIDACIÓN FINAL
            # ============================================================
            print("\n" + "="*80)
            print("✅ PIPELINE COMPLETADO")
            print("="*80)
            print(f"📊 Registros finales: {len(data_final):,}")
            print(f"📊 Columnas finales: {len(data_final.columns)}")
            
            if 'desercion' in data_final.columns:
                desercion_count = (data_final['desercion'] == 1).sum()
                tasa = (desercion_count / len(data_final)) * 100
                print(f"⚠️ Deserciones en datos: {desercion_count:,} ({tasa:.2f}%)")
            
            print("="*80)
            
            if progress_callback:
                progress_callback(1.0, "✅ Pipeline completado!")
            
            return data_final, logs
            
        except Exception as e:
            error_msg = f"❌ Error en pipeline: {str(e)}"
            logs["errores"].append(error_msg)
            print(error_msg)
            raise


def ejecutar_pipeline_streamlit(notas_df, per_df, prom_df, adm_df) -> pd.DataFrame:
    """
    Función simplificada para usar en Streamlit con barra de progreso
    
    Args:
        notas_df, per_df, prom_df, adm_df: DataFrames de entrada
        
    Returns:
        DataFrame final listo para predicción
    """
    
    # Crear barra de progreso
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(progress, message):
        progress_bar.progress(progress)
        status_text.text(message)
    
    try:
        # Ejecutar pipeline
        pipeline = PipelineIntegrado()
        data_final, logs = pipeline.procesar_completo(
            notas_df, per_df, prom_df, adm_df,
            progress_callback=update_progress
        )
        
        # Limpiar barra de progreso
        progress_bar.empty()
        status_text.empty()
        
        return data_final
        
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        raise e


# ============================================================================
# FUNCIÓN PARA VALIDAR EXCEL
# ============================================================================

def validar_excel(uploaded_file) -> Tuple[bool, str, dict]:
    """
    Valida que el Excel tenga las 4 hojas necesarias
    
    Args:
        uploaded_file: Archivo subido por Streamlit
        
    Returns:
        Tuple (es_valido, mensaje, dataframes)
    """
    try:
        # Leer todas las hojas
        excel_file = pd.ExcelFile(uploaded_file)
        hojas_disponibles = excel_file.sheet_names
        
        # Verificar hojas requeridas
        hojas_requeridas = ['NOTAS', 'PER', 'PROM', 'ADM']
        hojas_faltantes = [h for h in hojas_requeridas if h not in hojas_disponibles]
        
        if hojas_faltantes:
            return False, f"❌ Faltan las siguientes hojas: {', '.join(hojas_faltantes)}", {}
        
        # Leer DataFrames
        dfs = {
            'NOTAS': pd.read_excel(uploaded_file, sheet_name='NOTAS'),
            'PER': pd.read_excel(uploaded_file, sheet_name='PER'),
            'PROM': pd.read_excel(uploaded_file, sheet_name='PROM'),
            'ADM': pd.read_excel(uploaded_file, sheet_name='ADM')
        }
        
        # Validar que no estén vacíos
        for nombre, df in dfs.items():
            if df.empty:
                return False, f"❌ La hoja '{nombre}' está vacía", {}
        
        # Información de las hojas
        info = f"""
        ✅ Excel válido
        
        📊 **NOTAS**: {len(dfs['NOTAS']):,} registros
        📊 **PER**: {len(dfs['PER']):,} registros  
        📊 **PROM**: {len(dfs['PROM']):,} registros
        📊 **ADM**: {len(dfs['ADM']):,} registros
        """
        
        return True, info, dfs
        
    except Exception as e:
        return False, f"❌ Error al leer el archivo: {str(e)}", {}


# ============================================================================
# EJEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    print("Pipeline Integrado - Listo para usar")
    print("\nUso:")
    print("  pipeline = PipelineIntegrado()")
    print("  data_final, logs = pipeline.procesar_completo(notas, per, prom, adm)")
