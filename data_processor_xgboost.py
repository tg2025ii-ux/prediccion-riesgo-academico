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
        """
        Limpia y codifica variables siguiendo el Pipeline__2_ exactamente
        Genera las 157 features que el modelo XGBoost espera
        """
        
        print("    🧹 Iniciando limpieza y encoding final...")
        print(f"       Columnas antes: {len(data.columns)}")
        
        # PASO 1: Resolver columnas duplicadas ANTES de encoding
        # Mantener solo las versiones _per o sin sufijo
        for col_base in ['Sexo', 'Colegio', 'Dpto Nacimiento', 'País Nacimiento',
                         'Ciudad (Dirección)', 'Acción', 'Motivo']:
            
            # Buscar todas las versiones
            col_per = f"{col_base}_per"
            col_prom = f"{col_base}_prom" 
            col_adm = f"{col_base}_adm"
            col_ppn = f"{col_base}_ppn"
            
            # Si existe _per, usar esa
            if col_per in data.columns:
                data.rename(columns={col_per: col_base}, inplace=True)
                # Eliminar otras versiones
                for col in [col_prom, col_adm, col_ppn]:
                    if col in data.columns:
                        data.drop(columns=[col], inplace=True)
            # Si no existe _per pero existe _ppn, usar esa
            elif col_ppn in data.columns:
                data.rename(columns={col_ppn: col_base}, inplace=True)
                for col in [col_prom, col_adm]:
                    if col in data.columns:
                        data.drop(columns=[col], inplace=True)
        
        # PASO 2: ENCODING - Crear variables dummy (SIGUIENDO EL PIPELINE)
        
        # 2.1 Tipo Admisión → ta_
        if 'Tipo Admisión' in data.columns:
            print("       → Encoding: Tipo Admisión")
            dummies = pd.get_dummies(data['Tipo Admisión'], prefix='ta')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Tipo Admisión'], inplace=True)
        
        # 2.2 Programa → p_
        if 'Programa' in data.columns:
            print("       → Encoding: Programa")
            dummies = pd.get_dummies(data['Programa'], prefix='p')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Programa'], inplace=True)
        
        # 2.3 Siglas Prog → s_
        if 'Siglas Prog' in data.columns:
            print("       → Encoding: Siglas Prog")
            dummies = pd.get_dummies(data['Siglas Prog'], prefix='s')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Siglas Prog'], inplace=True)
        
        # 2.4 Ciudad (Dirección) → cd_
        if 'Ciudad (Dirección)' in data.columns:
            print("       → Encoding: Ciudad (Dirección)")
            dummies = pd.get_dummies(data['Ciudad (Dirección)'], prefix='cd')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Ciudad (Dirección)'], inplace=True)
        
        # 2.5 Dpto Nacimiento → dn_
        if 'Dpto Nacimiento' in data.columns:
            print("       → Encoding: Dpto Nacimiento")
            dummies = pd.get_dummies(data['Dpto Nacimiento'], prefix='dn')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Dpto Nacimiento'], inplace=True)
        
        # 2.6 País Nacimiento → pn_
        if 'País Nacimiento' in data.columns:
            print("       → Encoding: País Nacimiento")
            dummies = pd.get_dummies(data['País Nacimiento'], prefix='pn')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['País Nacimiento'], inplace=True)
        
        # 2.7 Acción → a_
        if 'Acción' in data.columns:
            print("       → Encoding: Acción")
            dummies = pd.get_dummies(data['Acción'], prefix='a')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Acción'], inplace=True)
        
        # 2.8 Motivo → m_
        if 'Motivo' in data.columns:
            print("       → Encoding: Motivo")
            dummies = pd.get_dummies(data['Motivo'], prefix='m')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Motivo'], inplace=True)
        
        # 2.9 Sexo → numérico (1 = Masculino, 0 = Femenino)
        if 'Sexo' in data.columns:
            print("       → Encoding: Sexo")
            data['Sexo'] = data['Sexo'].replace({'Masculino': 1, 'Femenino': 0})
        
        # 2.10 Edad → rangos (según map_age_groups del pipeline)
        if 'Edad' in data.columns:
            print("       → Encoding: Edad (rangos)")
            def map_age_groups(age):
                if pd.isna(age):
                    return 0
                if age <= 19:
                    return 0
                elif 20 <= age <= 22:
                    return 1
                elif 23 <= age <= 25:
                    return 2
                else:
                    return 3
            
            data['rango_edad'] = data['Edad'].apply(map_age_groups).astype('int8')
            data.drop(columns=['Edad'], inplace=True)
        
        # 2.11 Internacional (si existe)
        if 'País Nacimiento' not in data.columns:  # Si ya se procesó
            if 'Internacional' in data.columns:
                print("       → Encoding: Internacional")
                data['Internacional'] = data['Internacional'].astype(int)
        
        # PASO 3: Eliminar columnas que definitivamente no son features
        cols_eliminar = [
            # Identificación
            'ID', 'Nombre', 'Nombre_ppn', 'Nombre_adm', '2º Nombre', 'Última',
            '2º Apellido', '2º Apellido_per', '2º Apellido_prom', 'Apellidos', 'Nombres',
            
            # Documentos
            'Tipo Doc ID', 'Tipo Doc ID_ppn', 'Tipo Doc ID_adm',
            'Doc ID', 'Doc Identidad', 'Tipo Doc Identidad',
            
            # Contacto
            'Dirección', 'Dirección 1', 'Dirección 2',
            'Teléfono', 'Teléfono_ppn', 'Teléfono_adm',
            'Correo-E', 'Correo-E_ppn', 'Correo-E_adm', 'Otro Correo E',
            'Celular Inscripción',
            
            # Fechas
            'F Nacimiento', 'F Nacimiento_ppn', 'F Nacimiento_adm', 'Fecha Grado',
            
            # Ubicación (ya encoded o eliminadas)
            'Estado (Dirección)', 'País (Dirección)', 'Ciudad Nacimiento', 'Lugar Nacimiento',
            
            # Colegio (texto)
            'Colegio', 'Colegio_ppn', 'Colegio_adm', 'ID Colegio',
            
            # Descripciones
            'Descripción', 'Org Acad', 'Tipo',
            
            # Duplicados de Prog Acad
            'Prog Acad', 'Prog Acad_ppn', 'Prog Acad_adm', 'Prog Acad.1',
            
            # Otros
            'Situacion Acad', 'Año', 'Año_per', 'Año_prom',
            'Estado_adm', 'Estado Clase', 'Estado',
            'Créd Inscritos xa PromedioCicl', 'Créd.Inscrtos Aprbdos PromCicl',
            'Num_Materias_Ciclo',
            
            # Ciclo Admisión duplicados (mantener solo uno)
            'Ciclo Admisión_per', 'Ciclo Admisión_prom'
        ]
        
        cols_encontradas = [c for c in cols_eliminar if c in data.columns]
        if cols_encontradas:
            print(f"       → Eliminando {len(cols_encontradas)} columnas innecesarias")
            data.drop(columns=cols_encontradas, inplace=True)
        
        # PASO 4: Eliminar columnas con sufijos duplicados restantes
        cols_sufijos = [col for col in data.columns 
                       if any(col.endswith(s) for s in ['_per', '_prom', '_adm', '_ppn', '_pprom', '_notas'])]
        if cols_sufijos:
            print(f"       → Eliminando {len(cols_sufijos)} columnas con sufijos")
            data.drop(columns=cols_sufijos, inplace=True)
        
        # PASO 5: Convertir fechas restantes a numérico
        for col in data.select_dtypes(include=['datetime64']).columns:
            try:
                data[col] = (data[col] - pd.Timestamp('1970-01-01')).dt.days
                data[col].fillna(0, inplace=True)
            except:
                data.drop(columns=[col], inplace=True)
        
        # PASO 6: Convertir columnas object restantes a numérico o eliminar
        for col in data.select_dtypes(include=['object']).columns:
            try:
                data[col] = pd.to_numeric(data[col], errors='coerce')
                data[col].fillna(0, inplace=True)
            except:
                print(f"       ⚠️  Eliminando columna no convertible: {col}")
                data.drop(columns=[col], inplace=True)
        
        print(f"       ✓ Columnas después: {len(data.columns)}")
        print(f"       ✓ Tipos: {data.dtypes.value_counts().to_dict()}")
        
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
            
            print(f"   📊 Shape de X: {X.shape}")
            print(f"   📊 Tipo de X: {type(X)}")
            
            # Estandarizar si existe scaler
            if self.scaler is not None:
                print("   🔧 Aplicando scaler...")
                X_scaled = self.scaler.transform(X)
                
                # IMPORTANTE: Convertir de numpy array a DataFrame
                # XGBoost necesita un DataFrame de Pandas con nombres de columnas
                X_scaled = pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
                print(f"   ✓ Datos estandarizados y convertidos a DataFrame")
            else:
                X_scaled = X
            
            print(f"   📊 Shape de X_scaled: {X_scaled.shape}")
            print(f"   📊 Tipo de X_scaled: {type(X_scaled)}")
            
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
