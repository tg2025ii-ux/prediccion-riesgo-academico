# -*- coding: utf-8 -*-
"""
Procesador de Datos con XGBoost para Predicción de Deserción
Implementa el pipeline completo de Pipeline__2_ + predicción con XGBoost
"""

import pandas as pd
import numpy as np
import joblib
import os
from typing import Dict, Tuple
import warnings
warnings.filterwarnings('ignore')


class DataProcessorXGBoost:
    """
    Clase para procesar datos de estudiantes y predecir deserción con XGBoost
    Implementa el proceso completo del Pipeline__2_.ipynb
    """
    
    def __init__(self, model_dir='.'):
        """
        Inicializa el procesador y carga el modelo XGBoost
        
        Args:
            model_dir: Directorio donde están los archivos del modelo (por defecto: raíz '.')
        """
        self.model_dir = model_dir
        self._cargar_modelo()
    
    def _cargar_modelo(self):
        """Carga el modelo XGBoost y archivos auxiliares"""
        try:
            modelo_path = 'xgboost_modelo.pkl'
            scaler_path = 'scaler.pkl'
            columnas_path = 'columnas.pkl'
            
            print("🔍 DEBUG: Iniciando carga del modelo...")
            print(f"   Ruta esperada: {modelo_path}")
            print(f"   Directorio actual: {os.getcwd()}")
            print(f"   Archivos en directorio: {os.listdir('.')[:10]}")
            
            # Intentar descargar si no existe
            if not os.path.exists(modelo_path):
                print("🔍 Modelo no encontrado localmente, intentando descargar...")
                self._descargar_modelo()
            
            # Verificar que existe después de descargar
            if not os.path.exists(modelo_path):
                raise FileNotFoundError(
                    f"❌ Modelo no encontrado: {modelo_path}\n"
                    f"   Verifica que el archivo se descargó correctamente de Google Drive\n"
                    f"   O sube 'xgboost_modelo.pkl' manualmente a la raíz del proyecto"
                )
            
            print(f"✓ Archivo encontrado: {modelo_path}")
            print(f"  Tamaño: {os.path.getsize(modelo_path) / 1024 / 1024:.2f} MB")
            
            # Cargar modelo
            print("  Cargando modelo con joblib...")
            self.modelo = joblib.load(modelo_path)
            print("✅ Modelo XGBoost cargado exitosamente")
            
            # Cargar scaler (opcional)
            if os.path.exists(scaler_path):
                self.scaler = joblib.load(scaler_path)
                print("✅ Scaler cargado")
            else:
                self.scaler = None
                print("⚠️  scaler.pkl no encontrado - continuando sin estandarización previa")
            
            # Cargar columnas (opcional)
            if os.path.exists(columnas_path):
                self.columnas_modelo = joblib.load(columnas_path)
                print("✅ Columnas del modelo cargadas")
            else:
                self.columnas_modelo = None
                print("⚠️  columnas.pkl no encontrado - usando todas las columnas disponibles")
            
        except Exception as e:
            print(f"❌ Error cargando modelo: {str(e)}")
            print(f"   Tipo de error: {type(e).__name__}")
            import traceback
            print(f"   Traceback completo:")
            traceback.print_exc()
            self.modelo = None
            self.scaler = None
            self.columnas_modelo = None
    
    def _descargar_modelo(self):
        """
        Descarga el modelo desde Google Drive si no existe localmente
        MÉTODO ALTERNATIVO: Usar descarga directa sin gdown
        """
        modelo_path = 'xgboost_modelo.pkl'
        
        if not os.path.exists(modelo_path):
            print("⬇️ Descargando modelo desde Google Drive...")
            print("   Tamaño: ~142 MB - Esto puede tomar 1-2 minutos")
            
            try:
                import gdown
                
                # ID del archivo en Google Drive
                file_id = "1VLySTpc2m4soxTEjTi7xUSJcXyrF00JF"
                
                # Método 1: Intentar con gdown normal
                url = f"https://drive.google.com/uc?id={file_id}"
                
                try:
                    gdown.download(url, modelo_path, quiet=False, fuzzy=True)
                    print("✅ Modelo descargado exitosamente")
                    return
                except Exception as e1:
                    print(f"⚠️ Método 1 falló: {str(e1)}")
                    
                    # Método 2: Intentar con cached download
                    try:
                        gdown.cached_download(url, modelo_path, quiet=False)
                        print("✅ Modelo descargado exitosamente (método 2)")
                        return
                    except Exception as e2:
                        print(f"⚠️ Método 2 falló: {str(e2)}")
                        
                        # Método 3: Descarga con requests
                        print("   Intentando método 3 (requests)...")
                        import requests
                        
                        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                        
                        session = requests.Session()
                        response = session.get(download_url, stream=True)
                        
                        # Manejar archivos grandes con confirmación
                        for key, value in response.cookies.items():
                            if key.startswith('download_warning'):
                                download_url = f"https://drive.google.com/uc?export=download&confirm={value}&id={file_id}"
                                response = session.get(download_url, stream=True)
                                break
                        
                        # Guardar archivo
                        with open(modelo_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=32768):
                                if chunk:
                                    f.write(chunk)
                        
                        print("✅ Modelo descargado exitosamente (método 3)")
                        
            except Exception as e:
                print(f"❌ Error descargando: {str(e)}")
                print("   Solución: Sube el archivo 'xgboost_modelo.pkl' manualmente")
                print("   al repositorio de GitHub (raíz del proyecto)")
        else:
            print("✅ Modelo ya existe localmente")
    
    def procesar_archivo_excel(self, archivo_path: str) -> pd.DataFrame:
        """
        Procesa un archivo Excel con 4 hojas (NOTAS, PER, PROM, ADM)
        
        Args:
            archivo_path: Ruta al archivo Excel con las 4 hojas
            
        Returns:
            DataFrame procesado y listo para predicción
        """
        print("📂 Leyendo archivo Excel...")
        
        notas = pd.read_excel(archivo_path, sheet_name='NOTAS')
        per = pd.read_excel(archivo_path, sheet_name='PER')
        prom = pd.read_excel(archivo_path, sheet_name='PROM')
        adm = pd.read_excel(archivo_path, sheet_name='ADM')
        
        print(f"  NOTAS: {len(notas)} registros")
        print(f"  PER: {len(per)} registros")
        print(f"  PROM: {len(prom)} registros")
        print(f"  ADM: {len(adm)} registros")
        
        return self._pipeline_completo(notas, per, prom, adm)
    
    def procesar_dataframes(self, notas_df, per_df, prom_df, adm_df) -> pd.DataFrame:
        """
        Procesa 4 DataFrames separados (para Streamlit)
        
        Args:
            notas_df: DataFrame de NOTAS
            per_df: DataFrame de PER
            prom_df: DataFrame de PROM
            adm_df: DataFrame de ADM
            
        Returns:
            DataFrame procesado
        """
        return self._pipeline_completo(notas_df, per_df, prom_df, adm_df)
    
    def _pipeline_completo(self, notas, per, prom, adm) -> pd.DataFrame:
        """
        Implementa el Pipeline__2_.ipynb completo
        """
        print("\n" + "="*70)
        print("🔄 INICIANDO PIPELINE DE PROCESAMIENTO")
        print("="*70)
        
        # FASE 1: Limpieza individual
        print("\n📊 FASE 1: Limpieza de bases individuales")
        notas = self._procesar_notas(notas)
        per, per_original = self._procesar_per(per)
        prom = self._procesar_prom(prom)
        adm = self._procesar_adm(adm)
        
        # FASE 2: Filtros generales
        print("\n🔍 FASE 2: Aplicando filtros")
        notas, per, prom, adm = self._aplicar_filtros(notas, per, prom, adm)
        
        # FASE 3: Intersección de IDs
        print("\n🔗 FASE 3: Filtrando IDs comunes")
        notas, per, prom, adm = self._filtrar_ids_comunes(notas, per, prom, adm)
        
        # FASE 4: Merge secuencial
        print("\n🔀 FASE 4: Fusionando bases")
        data_final = self._fusionar_bases(notas, per, prom, adm)
        
        # FASE 5: Limpieza post-merge
        print("\n🧹 FASE 5: Limpieza post-fusión")
        data_final = self._limpieza_final(data_final)
        
        print("\n" + "="*70)
        print(f"✅ PIPELINE COMPLETADO: {len(data_final)} registros procesados")
        print("="*70 + "\n")
        
        return data_final
    
    def _procesar_notas(self, notas):
        """Procesa la base NOTAS"""
        print("  📋 Procesando NOTAS...")
        print(f"    📌 Columnas ANTES del rename: {list(notas.columns)}")
        
        # PASO 1: RENOMBRAR COLUMNAS (NOMBRES EXACTOS del Excel del usuario)
        rename_dict = {
            'Grado Académico': 'Mult Programa',  # ← CORREGIDO (con acento y espacio)
            'Programa Académico Base': 'Programa',  # ← CORREGIDO (con espacio)
            'Promedio_Ciclo': 'Promedio Ciclo',
            'Estado.1': 'Estado Clase'
        }
        notas.rename(columns=rename_dict, inplace=True)
        print(f"    📌 Columnas DESPUÉS del rename: {list(notas.columns)}")
        
        # PASO 2: Eliminar columnas innecesarias
        cols_drop = ['Nombre', 'Nº Oferta', 'Nº Clase', 'Sesión', 'Sección', 'Motivo']
        notas.drop(columns=[c for c in cols_drop if c in notas.columns], inplace=True)
        
        # PASO 3: Agrupar y crear métricas
        if all(c in notas.columns for c in ['ID', 'Programa', 'Ciclo']):
            grouped = notas.groupby(["ID", "Programa", "Ciclo"]).agg(
                Num_Materias_Ciclo=("ID", "count"),
                Cant_Perdidas=("Calif", lambda x: (x < 3).sum() if 'Calif' in notas.columns else 0),
                Materias_Vistas=("Estado", lambda x: (x == "E").sum() if 'Estado' in notas.columns else 0)
            ).reset_index()
            
            notas = notas.merge(grouped, on=["ID", "Programa", "Ciclo"], how="left")
        
        print(f"    ✓ NOTAS procesadas: {len(notas)} registros")
        return notas
    
    def _procesar_per(self, per):
        """Procesa la base PER"""
        print("  👤 Procesando PER...")
        print(f"    📌 Columnas ANTES del rename: {list(per.columns)}")
        
        # PASO 1: RENOMBRAR COLUMNAS (del Pipeline__2_)
        rename_dict = {
            'Grado Académico': 'Mult Programa',
            'Matrd Progr': 'Créditos Inscritos en Ciclo',
            'Cred. Aprob.': 'Créd.Inscritos y Aprobados Ciclo',
            'Ccl Admis': 'Ciclo Admisión',
            'Lugar Nacimiento': 'Ciudad Nacimiento',
            'Acc Prog': 'Acción',
            'Motivo Acción': 'Motivo'
        }
        per.rename(columns=rename_dict, inplace=True)
        print(f"    📌 Columnas DESPUÉS del rename: {list(per.columns)}")
        
        per_original = per.copy()  # Guardar copia original
        print(f"    ✓ PER procesada: {len(per)} registros")
        return per, per_original
    
    def _procesar_prom(self, prom):
        """Procesa la base PROM"""
        print("  📈 Procesando PROM...")
        print(f"    📌 Columnas ANTES del rename: {list(prom.columns)}")
        
        # PASO 1: RENOMBRAR COLUMNAS (del Pipeline__2_)
        rename_dict = {
            'Grado': 'Mult Programa',
            'Situacion Academica': 'Situacion Acad',
            'Créd.Inscrtos y Aprobdos Ciclo': 'Créd.Inscritos y Aprobados Ciclo',
            'Estado Programa Académico': 'Estado',
            'Acción Programa': 'Acción',
            'Motivo Accion': 'Motivo'
        }
        prom.rename(columns=rename_dict, inplace=True)
        print(f"    📌 Columnas DESPUÉS del rename: {list(prom.columns)}")
        
        print(f"    ✓ PROM procesada: {len(prom)} registros")
        return prom
    
    def _procesar_adm(self, adm):
        """Procesa la base ADM"""
        print("  🎓 Procesando ADM...")
        
        # PASO 1: RENOMBRAR COLUMNAS (del Pipeline__2_)
        rename_dict = {
            'Ciclo': 'Ciclo Admisión',
            'País': 'País Nacimiento',
            'Estado': 'Dpto Nacimiento',
            'Programa Académico': 'Programa',
            'Ciudad': 'Ciudad (Dirección)',
            'ID Org Ext': 'ID Colegio',
            'Descr': 'Colegio',
            'Estado.1': 'Estado'
        }
        adm.rename(columns=rename_dict, inplace=True)
        
        # PASO 2: Filtrar solo estudiantes activos
        if 'Estado' in adm.columns:
            adm = adm[adm["Estado"] == "Activo en Programa"].copy()
            print(f"    → Filtrados estudiantes activos")
        
        print(f"    ✓ ADM procesada: {len(adm)} registros")
        return adm
    
    def _aplicar_filtros(self, notas, per, prom, adm):
        """Aplica filtros generales a todas las bases"""
        
        # Filtrar ciclos máximos
        if 'Ciclo' in per.columns:
            ciclo_max = per["Ciclo"].max()
            per = per[per["Ciclo"] != ciclo_max]
            notas = notas[notas["Ciclo"] != ciclo_max] if 'Ciclo' in notas.columns else notas
            print(f"    → Ciclo máximo PER eliminado: {ciclo_max}")
        
        if 'Ciclo' in prom.columns:
            ciclo_max = prom["Ciclo"].max()
            prom = prom[prom["Ciclo"] != ciclo_max]
            print(f"    → Ciclo máximo PROM eliminado: {ciclo_max}")
        
        # Eliminar UCollege
        if 'Programa' in prom.columns:
            prom = prom[prom["Programa"] != "UCollege Javeriano"]
        if 'Programa' in per.columns:
            per = per[per["Programa"] != "UCollege Javeriano"]
        if 'Programa Académico' in adm.columns:
            adm = adm[adm["Programa Académico"] != "UCollege Javeriano"]
        
        print("    → UCollege eliminado")
        
        # Filtrar solo ciclos que terminan en 10 o 30
        for df_name, df in [('PER', per), ('PROM', prom), ('ADM', adm)]:
            if 'Ciclo' in df.columns:
                df_filtered = df[df['Ciclo'].astype(str).str.endswith(('10', '30'))]
                if df_name == 'PER':
                    per = df_filtered
                elif df_name == 'PROM':
                    prom = df_filtered
                else:
                    adm = df_filtered
        
        print("    → Ciclos filtrados (solo 10 y 30)")
        
        # Filtrar créditos = 0
        if 'Créditos Inscritos en Ciclo' in per.columns:
            per = per[per["Créditos Inscritos en Ciclo"] != 0]
        if 'Créditos Inscritos en Ciclo' in prom.columns:
            prom = prom[prom["Créditos Inscritos en Ciclo"] != 0]
        
        print("    → Registros con 0 créditos eliminados")
        
        return notas, per, prom, adm
    
    def _filtrar_ids_comunes(self, notas, per, prom, adm):
        """Filtra solo IDs presentes en las 4 bases"""
        ids_comunes = (
            set(notas["ID"]) & 
            set(per["ID"]) & 
            set(prom["ID"]) & 
            set(adm["ID"])
        )
        
        notas = notas[notas["ID"].isin(ids_comunes)]
        per = per[per["ID"].isin(ids_comunes)]
        prom = prom[prom["ID"].isin(ids_comunes)]
        adm = adm[adm["ID"].isin(ids_comunes)]
        
        print(f"    → IDs comunes: {len(ids_comunes)}")
        
        return notas, per, prom, adm
    
    def _fusionar_bases(self, notas, per, prom, adm):
        """Fusiona las 4 bases secuencialmente"""
        
        # Merge 1: PER + PROM
        per_prom = per.merge(
            prom,
            on=['ID', 'Mult Programa', 'Programa', 'Ciclo'],
            how='inner',
            suffixes=('_per', '_prom')
        )
        print(f"    1. PER + PROM = {len(per_prom)} registros")
        
        # Merge 2: (PER+PROM) + NOTAS
        per_prom_notas = per_prom.merge(
            notas,
            on=['ID', 'Mult Programa', 'Programa', 'Ciclo'],
            how='left',
            suffixes=('_pprom', '_notas')
        )
        print(f"    2. (PER+PROM) + NOTAS = {len(per_prom_notas)} registros")
        
        # Merge 3: (PER+PROM+NOTAS) + ADM
        data_final = per_prom_notas.merge(
            adm,
            on=['ID', 'Programa'],
            how='left',
            suffixes=('_ppn', '_adm')
        )
        print(f"    3. (PER+PROM+NOTAS) + ADM = {len(data_final)} registros")
        
        return data_final
    
    def _limpieza_final(self, data):
        """Limpia columnas duplicadas y renombra"""
        
        # Resolver columnas duplicadas
        # Preferir _per > _prom > _adm
        for col_base in ['Créditos Inscritos y Aprobados Ciclo', 'Ciudad (Dirección)', 
                         'Sexo', 'Colegio', 'F Nacimiento', 'Dpto Nacimiento', 'País Nacimiento']:
            
            col_per = f"{col_base}_per"
            col_prom = f"{col_base}_prom"
            col_adm = f"{col_base}_adm"
            col_ppn = f"{col_base}_ppn"
            
            # Si existe versión _per, usarla y eliminar las demás
            if col_per in data.columns:
                if col_prom in data.columns:
                    data.drop(columns=[col_prom], inplace=True)
                if col_adm in data.columns:
                    data.drop(columns=[col_adm], inplace=True)
                if col_ppn in data.columns:
                    data.drop(columns=[col_ppn], inplace=True)
                
                # Renombrar _per al nombre base
                data.rename(columns={col_per: col_base}, inplace=True)
        
        # Eliminar Ciclo Admisión duplicados
        if 'Ciclo Admisión_per' in data.columns:
            data.drop(columns=['Ciclo Admisión_per'], inplace=True, errors='ignore')
        if 'Ciclo Admisión_prom' in data.columns:
            data.drop(columns=['Ciclo Admisión_prom'], inplace=True, errors='ignore')
        
        # Resolver Estado, Acción, Motivo (preferir _per)
        for col in ['Estado', 'Acción', 'Motivo']:
            if f"{col}_per" in data.columns:
                data.drop(columns=[f"{col}_prom", f"{col}_ppn"], inplace=True, errors='ignore')
                data.rename(columns={f"{col}_per": col}, inplace=True)
        
        # Eliminar columnas innecesarias
        cols_eliminar = [
            'Fecha Grado', 'Estado_adm', 'Situacion Acad', 'ID Colegio',
            'Créd Inscritos xa PromedioCicl', 'Créd.Inscrtos Aprbdos PromCicl',
            'Num_Materias_Ciclo'
        ]
        data.drop(columns=[c for c in cols_eliminar if c in data.columns], inplace=True, errors='ignore')
        
        print("    ✓ Limpieza de columnas completada")
        
        return data
    
    def predecir(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza predicciones con el modelo XGBoost (potencialmente envuelto en ExponentiatedGradient)
        
        Args:
            data: DataFrame procesado por el pipeline
            
        Returns:
            DataFrame con probabilidades y nivel de riesgo
        """
        print("\n🎯 INICIANDO PREDICCIÓN...")
        print(f"   Estado del modelo: {'✅ Cargado' if self.modelo is not None else '❌ NO cargado'}")
        print(f"   Tipo de modelo: {type(self.modelo).__name__}")
        print(f"   Registros a predecir: {len(data)}")
        
        if self.modelo is None:
            raise ValueError(
                "❌ Modelo no cargado. Verifica que xgboost_modelo.pkl exista en la raíz del proyecto\n"
                "   El modelo debería haberse descargado automáticamente desde Google Drive.\n"
                "   Revisa los logs para ver si hubo errores en la descarga o carga."
            )
        
        print("🎯 Realizando predicciones...")
        
        try:
            # Preparar datos para el modelo
            if self.columnas_modelo is not None:
                # Asegurar que todas las columnas existan
                for col in self.columnas_modelo:
                    if col not in data.columns:
                        data[col] = 0  # Agregar columnas faltantes con 0
                
                X = data[self.columnas_modelo]
            else:
                X = data
            
            # Estandarizar si existe scaler
            if self.scaler is not None:
                X_scaled = self.scaler.transform(X)
            else:
                X_scaled = X
            
            # Detectar si es ExponentiatedGradient o modelo normal
            modelo_tipo = type(self.modelo).__name__
            
            if 'ExponentiatedGradient' in modelo_tipo or 'GridSearch' in modelo_tipo:
                print("   ℹ️ Detectado modelo con mitigación (ExponentiatedGradient)")
                
                # ExponentiatedGradient solo tiene predict(), no predict_proba()
                # Usar el método predict() que devuelve 0 o 1
                predicciones = self.modelo.predict(X_scaled)
                
                # Convertir a probabilidades (0 o 1)
                # Como no tenemos probabilidades reales, usamos las predicciones directas
                # Asignamos probabilidades artificiales: 0.1 para clase 0, 0.9 para clase 1
                probabilidades = np.where(predicciones == 1, 0.9, 0.1)
                
                print("   ⚠️ Usando predicciones binarias (0/1) convertidas a probabilidades aproximadas")
                
            else:
                print("   ℹ️ Detectado modelo estándar con predict_proba()")
                # Modelo normal con predict_proba
                probabilidades = self.modelo.predict_proba(X_scaled)[:, 1]
            
            # Agregar resultados al DataFrame
            resultado = data.copy()
            resultado['probabilidad'] = probabilidades
            resultado['nivel_riesgo'] = pd.cut(
                probabilidades,
                bins=[0, 0.3, 0.6, 1.0],
                labels=["Bajo", "Medio", "Alto"]
            )
            
            print(f"✅ Predicciones completadas: {len(resultado)} estudiantes")
            print(f"   🟢 Bajo: {(resultado['nivel_riesgo']=='Bajo').sum()}")
            print(f"   🟡 Medio: {(resultado['nivel_riesgo']=='Medio').sum()}")
            print(f"   🔴 Alto: {(resultado['nivel_riesgo']=='Alto').sum()}")
            
            return resultado
            
        except Exception as e:
            print(f"❌ Error en predicción: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def get_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Calcula estadísticas resumidas"""
        return {
            "total_estudiantes": len(df),
            "riesgo_alto": len(df[df["probabilidad"] > 0.6]),
            "riesgo_medio": len(df[(df["probabilidad"] >= 0.3) & (df["probabilidad"] <= 0.6)]),
            "riesgo_bajo": len(df[df["probabilidad"] < 0.3]),
            "probabilidad_promedio": df["probabilidad"].mean(),
            "probabilidad_max": df["probabilidad"].max(),
            "probabilidad_min": df["probabilidad"].min()
        }
    
    def generar_estadisticas_descriptivas(self, data_original: Dict[str, pd.DataFrame]) -> Dict:
        """
        Genera estadísticas de los datos ORIGINALES ingresados
        
        Args:
            data_original: Dict con {'notas': df, 'per': df, 'prom': df, 'adm': df}
        
        Returns:
            Dict con estadísticas para gráficas
        """
        stats = {}
        
        # De PER
        if 'per' in data_original:
            per = data_original['per']
            
            if 'Sexo' in per.columns:
                stats['sexo'] = per['Sexo'].value_counts().to_dict()
            
            if 'Edad' in per.columns:
                stats['edad_promedio'] = per['Edad'].mean()
                stats['edad_std'] = per['Edad'].std()
        
        # De PROM
        if 'prom' in data_original:
            prom = data_original['prom']
            
            if 'Promedio Acumulado' in prom.columns:
                stats['promedio_general'] = prom['Promedio Acumulado'].mean()
        
        # De ADM
        if 'adm' in data_original:
            adm = data_original['adm']
            
            if 'Programa Académico' in adm.columns:
                stats['top_programas'] = adm['Programa Académico'].value_counts().head(10).to_dict()
        
        return stats
