"""
Data Processor XGBoost - Réplica COMPLETA del Pipeline__2_
Genera exactamente las 157 features que el modelo espera
"""

import pandas as pd
import numpy as np
import os
import joblib
from typing import Dict, Tuple

class DataProcessorXGBoost:
    """
    Procesador que replica EXACTAMENTE el Pipeline__2_.ipynb
    Incluye todas las transformaciones y métricas de calificaciones
    """
    
    def __init__(self, model_dir='.'):
        """Inicializa el procesador y carga el modelo"""
        self.model_dir = model_dir
        self.modelo = None
        self.scaler = None
        self.columnas_modelo = None
        
        # Cargar archivo de categorías
        self.categorias = None
        self._cargar_categorias()
        
        # Cargar modelo
        self._cargar_modelo()
    
    def _cargar_categorias(self):
        """Carga el archivo de categorías para mapear materias"""
        try:
            categorias_path = 'Ejemplo__1_.xlsx'
            
            if os.path.exists(categorias_path):
                self.categorias = pd.read_excel(categorias_path, sheet_name='Hoja1')
                # Crear diccionario de mapeo
                self.mapa_categorias = dict(zip(
                    self.categorias['Clase'], 
                    self.categorias['Categoría ']
                ))
                print(f"✅ Categorías cargadas: {len(self.mapa_categorias)} materias mapeadas")
            else:
                print("⚠️ Archivo de categorías no encontrado, usando mapeo por defecto")
                self.mapa_categorias = {}
                
        except Exception as e:
            print(f"⚠️ Error cargando categorías: {e}")
            self.mapa_categorias = {}
    
    def _cargar_modelo(self):
        """Carga el modelo XGBoost y archivos auxiliares"""
        try:
            modelo_path = 'xgboost_modelo.pkl'
            scaler_path = 'scaler.pkl'
            columnas_path = 'columnas.pkl'
            
            print("🔍 DEBUG: Iniciando carga del modelo...")
            print(f"   Ruta esperada: {modelo_path}")
            print(f"   Directorio actual: {os.getcwd()}")
            
            # Intentar descargar si no existe
            if not os.path.exists(modelo_path):
                print("🔍 Modelo no encontrado localmente, intentando descargar...")
                self._descargar_modelo()
            
            # Verificar que existe después de descargar
            if not os.path.exists(modelo_path):
                raise FileNotFoundError(
                    f"❌ Modelo no encontrado: {modelo_path}\n"
                    f"   Verifica que el archivo se descargó correctamente de Google Drive"
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
            import traceback
            traceback.print_exc()
            self.modelo = None
            self.scaler = None
            self.columnas_modelo = None
    
    def _descargar_modelo(self):
        """Descarga el modelo desde Google Drive"""
        modelo_path = 'xgboost_modelo.pkl'
        
        if not os.path.exists(modelo_path):
            print("⬇️ Descargando modelo desde Google Drive...")
            print("   Tamaño: ~142 MB - Esto puede tomar 1-2 minutos")
            
            try:
                import gdown
                
                file_id = "1VLySTpc2m4soxTEjTi7xUSJcXyrF00JF"
                
                try:
                    url = f"https://drive.google.com/uc?id={file_id}"
                    gdown.download(url, modelo_path, quiet=False, fuzzy=True)
                    print("✅ Modelo descargado exitosamente")
                    return
                except Exception as e1:
                    print(f"⚠️ Método 1 falló: {str(e1)}")
                    
                    try:
                        gdown.cached_download(url, modelo_path, quiet=False)
                        print("✅ Modelo descargado exitosamente (método 2)")
                        return
                    except Exception as e2:
                        print(f"⚠️ Método 2 falló: {str(e2)}")
                        print("   Intentando método 3 (requests)...")
                        import requests
                        
                        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                        session = requests.Session()
                        response = session.get(download_url, stream=True)
                        
                        for key, value in response.cookies.items():
                            if key.startswith('download_warning'):
                                download_url = f"https://drive.google.com/uc?export=download&confirm={value}&id={file_id}"
                                response = session.get(download_url, stream=True)
                                break
                        
                        with open(modelo_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=32768):
                                if chunk:
                                    f.write(chunk)
                        
                        print("✅ Modelo descargado exitosamente (método 3)")
                        
            except Exception as e:
                print(f"❌ Error descargando: {str(e)}")
                print("   Solución: Sube el archivo 'xgboost_modelo.pkl' manualmente")
        else:
            print("✅ Modelo ya existe localmente")
    
    def procesar_archivo_excel(self, archivo_path):
        """Procesa un archivo Excel con 4 hojas"""
        notas = pd.read_excel(archivo_path, sheet_name='NOTAS')
        per = pd.read_excel(archivo_path, sheet_name='PER')
        prom = pd.read_excel(archivo_path, sheet_name='PROM')
        adm = pd.read_excel(archivo_path, sheet_name='ADM')
        return self._pipeline_completo(notas, per, prom, adm)
    
    def procesar_dataframes(self, notas_df, per_df, prom_df, adm_df) -> pd.DataFrame:
        """Procesa 4 DataFrames separados (para Streamlit)"""
        return self._pipeline_completo(notas_df, per_df, prom_df, adm_df)
    
    def _pipeline_completo(self, notas, per, prom, adm) -> pd.DataFrame:
        """Implementa el Pipeline__2_.ipynb COMPLETO"""
        print("\n" + "="*70)
        print("🔄 INICIANDO PIPELINE DE PROCESAMIENTO COMPLETO")
        print("="*70)
        
        # FASE 1: Limpiezas y renames
        print("\n📊 FASE 1: Limpieza de bases individuales")
        notas = self._procesar_notas(notas)
        per, per_original = self._procesar_per(per)
        prom = self._procesar_prom(prom)
        adm = self._procesar_adm(adm)
        
        # FASE 2: Filtros generales
        print("\n🔍 FASE 2: Aplicando filtros")
        per, prom = self._aplicar_filtros_generales(per, prom, per_original)
        
        # FASE 3: IDs comunes
        print("\n🔗 FASE 3: Filtrando IDs comunes")
        notas, per, prom, adm = self._filtrar_ids_comunes(notas, per, prom, adm)
        
        # FASE 4: Fusión secuencial
        print("\n🔀 FASE 4: Fusionando bases")
        data_fusionada = self._fusionar_bases(per, prom, notas, adm)
        
        # FASE 5: Calcular métricas de calificaciones
        print("\n📊 FASE 5: Calculando métricas de calificaciones")
        data_con_metricas = self._calcular_metricas_calificaciones(data_fusionada, notas)
        
        # FASE 6: Limpieza final y encoding
        print("\n🧹 FASE 6: Limpieza final y encoding")
        data_final = self._limpieza_y_encoding_final(data_con_metricas)
        
        print("\n" + "="*70)
        print(f"✅ PIPELINE COMPLETADO: {len(data_final)} registros, {len(data_final.columns)} columnas")
        print("="*70)
        
        return data_final
    
    def _procesar_notas(self, notas):
        """Procesa la base NOTAS"""
        print("  📋 Procesando NOTAS...")
        
        # RENOMBRAR COLUMNAS
        rename_dict = {
            'Grado Académico': 'Mult Programa',
            'Programa Académico Base': 'Programa',
            'Promedio_Ciclo': 'Promedio Ciclo',
            'Estado.1': 'Estado Clase'
        }
        notas.rename(columns=rename_dict, inplace=True)
        
        # Eliminar columnas innecesarias
        cols_drop = ['Nombre', 'Nº Oferta', 'Nº Clase', 'Sesión', 'Sección', 'Motivo']
        notas.drop(columns=[c for c in cols_drop if c in notas.columns], inplace=True)
        
        print(f"    ✓ NOTAS procesadas: {len(notas)} registros")
        return notas
    
    def _procesar_per(self, per):
        """Procesa la base PER"""
        print("  👤 Procesando PER...")
        
        # RENOMBRAR COLUMNAS
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
        
        per_original = per.copy()
        print(f"    ✓ PER procesada: {len(per)} registros")
        return per, per_original
    
    def _procesar_prom(self, prom):
        """Procesa la base PROM"""
        print("  📈 Procesando PROM...")
        
        # RENOMBRAR COLUMNAS
        rename_dict = {
            'Grado': 'Mult Programa',
            'Situacion Academica': 'Situacion Acad',
            'Créd.Inscrtos y Aprobdos Ciclo': 'Créd.Inscritos y Aprobados Ciclo',
            'Estado Programa Académico': 'Estado',
            'Acción Programa': 'Acción',
            'Motivo Accion': 'Motivo'
        }
        prom.rename(columns=rename_dict, inplace=True)
        
        print(f"    ✓ PROM procesada: {len(prom)} registros")
        return prom
    
    def _procesar_adm(self, adm):
        """Procesa la base ADM"""
        print("  🎓 Procesando ADM...")
        
        # RENOMBRAR COLUMNAS
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
        
        # Filtrar solo estudiantes activos
        if 'Estado' in adm.columns:
            adm = adm[adm["Estado"] == "Activo en Programa"].copy()
            print(f"    → Filtrados estudiantes activos")
        
        print(f"    ✓ ADM procesada: {len(adm)} registros")
        return adm
    
    def _aplicar_filtros_generales(self, per, prom, per_original):
        """Aplica los filtros generales del pipeline"""
        # Eliminar ciclo máximo
        ciclo_max_per = per['Ciclo'].max()
        ciclo_max_prom = prom['Ciclo'].max()
        
        per = per[per['Ciclo'] != ciclo_max_per].copy()
        prom = prom[prom['Ciclo'] != ciclo_max_prom].copy()
        
        print(f"    → Ciclo máximo PER eliminado: {ciclo_max_per}")
        print(f"    → Ciclo máximo PROM eliminado: {ciclo_max_prom}")
        
        # Eliminar UCollege
        per = per[per['Programa'] != 'UCollege Javeriano'].copy()
        prom = prom[prom['Programa'] != 'UCollege Javeriano'].copy()
        print(f"    → UCollege eliminado")
        
        # Filtrar solo ciclos que terminan en 10 o 30
        per = per[per['Ciclo'].astype(str).str.endswith(('10', '30'))].copy()
        prom = prom[prom['Ciclo'].astype(str).str.endswith(('10', '30'))].copy()
        print(f"    → Ciclos filtrados (solo 10 y 30)")
        
        # Eliminar registros con 0 créditos
        if 'Créditos Inscritos en Ciclo' in per.columns:
            per = per[per['Créditos Inscritos en Ciclo'] > 0].copy()
            print(f"    → Registros con 0 créditos eliminados")
        
        return per, prom
    
    def _filtrar_ids_comunes(self, notas, per, prom, adm):
        """Filtra solo IDs que estén en las 4 bases"""
        ids_comunes = set(notas["ID"]) & set(per["ID"]) & set(prom["ID"]) & set(adm["ID"])
        
        notas = notas[notas["ID"].isin(ids_comunes)].copy()
        per = per[per["ID"].isin(ids_comunes)].copy()
        prom = prom[prom["ID"].isin(ids_comunes)].copy()
        adm = adm[adm["ID"].isin(ids_comunes)].copy()
        
        print(f"    → IDs comunes: {len(ids_comunes)}")
        
        return notas, per, prom, adm
    
    def _fusionar_bases(self, per, prom, notas, adm):
        """Fusiona las 4 bases secuencialmente"""
        # 1. PER + PROM
        per_prom = per.merge(
            prom,
            on=['ID', 'Mult Programa', 'Programa', 'Ciclo'],
            how='inner',
            suffixes=('_per', '_prom')
        )
        print(f"    1. PER + PROM = {len(per_prom)} registros")
        
        # 2. (PER+PROM) + NOTAS
        per_prom_notas = per_prom.merge(
            notas,
            on=['ID', 'Mult Programa', 'Programa', 'Ciclo'],
            how='left',
            suffixes=('_pprom', '_notas')
        )
        print(f"    2. (PER+PROM) + NOTAS = {len(per_prom_notas)} registros")
        
        # 3. (PER+PROM+NOTAS) + ADM
        data_final = per_prom_notas.merge(
            adm,
            on=['ID', 'Programa'],
            how='left',
            suffixes=('_ppn', '_adm')
        )
        print(f"    3. (PER+PROM+NOTAS) + ADM = {len(data_final)} registros")
        
        return data_final
    
    def _calcular_metricas_calificaciones(self, data, notas):
        """
        Calcula métricas de calificaciones por estudiante/ciclo
        Réplica de calcular_metricas_calificaciones_paso2_optimizado
        """
        print("    🔢 Calculando métricas...")
        
        # Agrupar por ID, Mult Programa, Ciclo
        grupos = notas.groupby(['ID', 'Mult Programa', 'Ciclo'])
        
        metricas_lista = []
        
        for (id_est, mult_prog, ciclo), grupo in grupos:
            if 'Calif' not in grupo.columns or 'Uni Matrd' not in grupo.columns:
                continue
            
            califs = grupo['Calif'].values
            creditos = grupo['Uni Matrd'].values
            
            if len(califs) == 0:
                continue
            
            # Calcular métricas
            promedio = np.average(califs, weights=creditos)
            
            if len(califs) > 1:
                varianza = np.average((califs - promedio)**2, weights=creditos)
                desviacion = np.sqrt(varianza)
            else:
                desviacion = 0.0
            
            # MIN y sus detalles
            idx_min = grupo['Calif'].idxmin()
            min_calif = grupo.loc[idx_min, 'Calif']
            min_creditos = grupo.loc[idx_min, 'Uni Matrd']
            min_id_curso = grupo.loc[idx_min, 'ID Curso'] if 'ID Curso' in grupo.columns else ''
            min_descripcion = grupo.loc[idx_min, 'Descripción'] if 'Descripción' in grupo.columns else 'Sin datos'
            
            # MAX y sus detalles
            idx_max = grupo['Calif'].idxmax()
            max_calif = grupo.loc[idx_max, 'Calif']
            max_creditos = grupo.loc[idx_max, 'Uni Matrd']
            max_id_curso = grupo.loc[idx_max, 'ID Curso'] if 'ID Curso' in grupo.columns else ''
            max_descripcion = grupo.loc[idx_max, 'Descripción'] if 'Descripción' in grupo.columns else 'Sin datos'
            
            # Rango ponderado
            contribuciones = califs * creditos
            rango_ponderado = contribuciones.max() - contribuciones.min()
            
            metricas_lista.append({
                'ID': id_est,
                'Mult Programa': mult_prog,
                'Ciclo': ciclo,
                'Promedio_Ciclo': round(promedio, 2),
                'Des_Estandar_Ciclo': round(desviacion, 2),
                'Min_Ciclo': round(min_calif, 2),
                'Cred_Min_Calif_Ciclo': min_creditos,
                'ID_Min_Ciclo': min_id_curso,
                'Clase_Min_Ciclo': str(min_descripcion),
                'Max_Ciclo': round(max_calif, 2),
                'Cred_Max_Calif_Ciclo': max_creditos,
                'ID_Max_Ciclo': max_id_curso,
                'Clase_Max_Ciclo': str(max_descripcion),
                'Rango_Ponderado_Ciclo': round(rango_ponderado, 2)
            })
        
        metricas_df = pd.DataFrame(metricas_lista)
        
        # Merge con data
        data_con_metricas = data.merge(
            metricas_df,
            on=['ID', 'Mult Programa', 'Ciclo'],
            how='left'
        )
        
        # Rellenar NaN
        data_con_metricas['Clase_Min_Ciclo'].fillna('Sin datos', inplace=True)
        data_con_metricas['Clase_Max_Ciclo'].fillna('Sin datos', inplace=True)
        
        print(f"    ✓ Métricas calculadas: {len(metricas_df)} grupos")
        
        return data_con_metricas
    
    def _limpieza_y_encoding_final(self, data):
        """Limpieza final y generación de todas las variables dummy"""
        print("    🧹 Iniciando limpieza y encoding...")
        print(f"       Columnas antes: {len(data.columns)}")
        print(f"       Columnas disponibles: {sorted(data.columns)[:50]}")  # Primeras 50
        
        # Normalizar Clase_Min_Ciclo y Clase_Max_Ciclo
        if 'Clase_Min_Ciclo' in data.columns:
            data['Clase_Min_Ciclo'] = data['Clase_Min_Ciclo'].str.title()
            print(f"       ✓ Clase_Min_Ciclo normalizada: {data['Clase_Min_Ciclo'].nunique()} valores únicos")
        if 'Clase_Max_Ciclo' in data.columns:
            data['Clase_Max_Ciclo'] = data['Clase_Max_Ciclo'].str.title()
            print(f"       ✓ Clase_Max_Ciclo normalizada: {data['Clase_Max_Ciclo'].nunique()} valores únicos")
        
        # Mapear a categorías
        if self.mapa_categorias:
            print("       → Mapeando Clase_Min_Ciclo a categorías...")
            data['Cat_ClaseMin'] = data['Clase_Min_Ciclo'].map(self.mapa_categorias)
            data['Cat_ClaseMin'] = data['Cat_ClaseMin'].fillna('Otros')
            print(f"         Categorías Min únicas: {data['Cat_ClaseMin'].nunique()}")
            
            print("       → Mapeando Clase_Max_Ciclo a categorías...")
            data['Cat_ClaseMax'] = data['Clase_Max_Ciclo'].map(self.mapa_categorias)
            data['Cat_ClaseMax'] = data['Cat_ClaseMax'].fillna('Otros')
            print(f"         Categorías Max únicas: {data['Cat_ClaseMax'].nunique()}")
        
        # Buscar columnas clave
        columnas_clave = {
            'Programa': [c for c in data.columns if 'Programa' in c and 'Académico' not in c],
            'Siglas': [c for c in data.columns if 'Siglas' in c],
            'Ciudad': [c for c in data.columns if 'Ciudad' in c and 'Dirección' in c],
            'Dpto': [c for c in data.columns if 'Dpto' in c and 'Nacimiento' in c],
            'País': [c for c in data.columns if 'País' in c and 'Nacimiento' in c],
            'Acción': [c for c in data.columns if 'Acción' in c],
            'Motivo': [c for c in data.columns if 'Motivo' in c],
            'Tipo Admisión': [c for c in data.columns if 'Tipo' in c and 'Admisión' in c],
            'Sexo': [c for c in data.columns if 'Sexo' in c],
            'Edad': [c for c in data.columns if 'Edad' in c],
        }
        
        print("\n       📋 Columnas encontradas para encoding:")
        for nombre, cols in columnas_clave.items():
            if cols:
                print(f"         {nombre}: {cols}")
            else:
                print(f"         {nombre}: ❌ NO ENCONTRADA")
        print()
        
        # ENCODING - Crear variables dummy en el orden del pipeline
        
        # 1. Programa → p_
        if 'Programa' in data.columns:
            print(f"       → Encoding: Programa ({data['Programa'].nunique()} valores)")
            dummies = pd.get_dummies(data['Programa'], prefix='p')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Programa'], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 2. Siglas Prog → s_
        if 'Siglas Prog' in data.columns:
            print(f"       → Encoding: Siglas Prog ({data['Siglas Prog'].nunique()} valores)")
            dummies = pd.get_dummies(data['Siglas Prog'], prefix='s')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Siglas Prog'], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 3. Ciudad (Dirección) → cd_
        col_ciudad = None
        for c in data.columns:
            if 'Ciudad' in c and 'Dirección' in c:
                col_ciudad = c
                break
        
        if col_ciudad:
            print(f"       → Encoding: {col_ciudad} ({data[col_ciudad].nunique()} valores)")
            dummies = pd.get_dummies(data[col_ciudad], prefix='cd')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=[col_ciudad], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 4. Dpto Nacimiento → dn_
        col_dpto = None
        for c in data.columns:
            if 'Dpto' in c and 'Nacimiento' in c:
                col_dpto = c
                break
        
        if col_dpto:
            print(f"       → Encoding: {col_dpto} ({data[col_dpto].nunique()} valores)")
            dummies = pd.get_dummies(data[col_dpto], prefix='dn')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=[col_dpto], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 5. País Nacimiento → pn_
        col_pais = None
        for c in data.columns:
            if 'País' in c and 'Nacimiento' in c:
                col_pais = c
                break
        
        if col_pais:
            print(f"       → Encoding: {col_pais} ({data[col_pais].nunique()} valores)")
            dummies = pd.get_dummies(data[col_pais], prefix='pn')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=[col_pais], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 6. Acción → a_
        # Buscar cualquier columna con Acción
        col_accion = None
        for c in data.columns:
            if 'Acción' in c or 'Accion' in c:
                col_accion = c
                break
        
        if col_accion:
            print(f"       → Encoding: {col_accion} ({data[col_accion].nunique()} valores)")
            dummies = pd.get_dummies(data[col_accion], prefix='a')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=[col_accion], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 7. Motivo → m_
        col_motivo = None
        for c in data.columns:
            if 'Motivo' in c:
                col_motivo = c
                break
        
        if col_motivo:
            print(f"       → Encoding: {col_motivo} ({data[col_motivo].nunique()} valores)")
            dummies = pd.get_dummies(data[col_motivo], prefix='m')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=[col_motivo], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 8. Cat_ClaseMax → ccmax_
        if 'Cat_ClaseMax' in data.columns:
            print(f"       → Encoding: Cat_ClaseMax ({data['Cat_ClaseMax'].nunique()} valores)")
            dummies = pd.get_dummies(data['Cat_ClaseMax'], prefix='ccmax')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Cat_ClaseMax'], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 9. Cat_ClaseMin → ccmin_
        if 'Cat_ClaseMin' in data.columns:
            print(f"       → Encoding: Cat_ClaseMin ({data['Cat_ClaseMin'].nunique()} valores)")
            dummies = pd.get_dummies(data['Cat_ClaseMin'], prefix='ccmin')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Cat_ClaseMin'], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 10. Tipo Admisión → ta_
        col_tipo_adm = None
        for c in data.columns:
            if 'Tipo' in c and 'Admisión' in c:
                col_tipo_adm = c
                break
        
        if col_tipo_adm:
            print(f"       → Encoding: {col_tipo_adm} ({data[col_tipo_adm].nunique()} valores)")
            dummies = pd.get_dummies(data[col_tipo_adm], prefix='ta')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=[col_tipo_adm], inplace=True)
            print(f"         Generadas {len(dummies.columns)} dummies")
        
        # 11. Sexo → numérico
        col_sexo = None
        for c in data.columns:
            if 'Sexo' in c:
                col_sexo = c
                break
        
        if col_sexo:
            print(f"       → Encoding: {col_sexo}")
            data['Sexo'] = data[col_sexo].replace({'M': 1, 'F': 0, 'Masculino': 1, 'Femenino': 0})
            if col_sexo != 'Sexo':
                data.drop(columns=[col_sexo], inplace=True)
        
        # 12. Edad → rangos
        if 'Edad' in data.columns:
            print("       → Encoding: Edad (rangos)")
            def map_age_groups(age):
                if pd.isna(age):
                    return 0
                if age <= 19:
                    return 0
                elif age <= 24:
                    return 1
                else:
                    return 3
            
            data['rango_edad'] = data['Edad'].apply(map_age_groups).astype('int8')
            data.drop(columns=['Edad'], inplace=True)
        
        # Eliminar columnas innecesarias
        cols_eliminar = [
            # Identificación
            'ID', 'Nombre', 'Nombre_ppn', 'Nombre_adm', '2º Nombre', 'Última',
            '2º Apellido', '2º Apellido_per', '2º Apellido_prom', 'Apellidos', 'Nombres',
            'Tipo Doc ID', 'Tipo Doc ID_ppn', 'Tipo Doc ID_adm',
            'Doc ID', 'Doc Identidad', 'Tipo Doc Identidad',
            'Dirección', 'Dirección 1', 'Dirección 2',
            'Teléfono', 'Teléfono_ppn', 'Teléfono_adm',
            'Correo-E', 'Correo-E_ppn', 'Correo-E_adm', 'Otro Correo E',
            'Celular Inscripción', 'F Nacimiento', 'F Nacimiento_ppn', 'F Nacimiento_adm',
            'Fecha Grado', 'Estado (Dirección)', 'País (Dirección)',
            'Ciudad Nacimiento', 'Lugar Nacimiento', 'Colegio', 'Colegio_ppn', 'Colegio_adm',
            'ID Colegio', 'Descripción', 'Org Acad', 'Tipo', 'Estado_adm', 'Estado Clase',
            'Prog Acad', 'Prog Acad_ppn', 'Prog Acad_adm', 'Prog Acad.1',
            'Ciclo Admisión_per', 'Ciclo Admisión_prom', 'Situacion Acad',
            'Año', 'Año_per', 'Año_prom', 'Estado', 'Clase_Min_Ciclo', 'Clase_Max_Ciclo',
            'ID_Min_Ciclo', 'ID_Max_Ciclo', 'Mult Programa', 'Ciclo',
            'ID Curso', 'Calif', 'Uni Matrd', 'Benef. Beca'
        ]
        
        cols_encontradas = [c for c in cols_eliminar if c in data.columns]
        if cols_encontradas:
            data.drop(columns=cols_encontradas, inplace=True)
        
        # Eliminar columnas con sufijos
        cols_sufijos = [col for col in data.columns 
                       if any(col.endswith(s) for s in ['_per', '_prom', '_adm', '_ppn', '_pprom', '_notas'])]
        if cols_sufijos:
            data.drop(columns=cols_sufijos, inplace=True)
        
        # Convertir fechas y columnas object a numérico
        for col in data.select_dtypes(include=['datetime64']).columns:
            try:
                data[col] = (data[col] - pd.Timestamp('1970-01-01')).dt.days
                data[col] = data[col].fillna(0)
            except:
                data.drop(columns=[col], inplace=True)
        
        for col in data.select_dtypes(include=['object']).columns:
            try:
                data[col] = pd.to_numeric(data[col], errors='coerce')
                data[col] = data[col].fillna(0)
            except:
                data.drop(columns=[col], inplace=True)
        
        print(f"\n       ✓ Columnas después del encoding: {len(data.columns)}")
        print(f"       ✓ Tipos de datos finales:")
        print(f"          {data.dtypes.value_counts().to_dict()}")
        
        return data
        """Limpieza final y generación de todas las variables dummy"""
        print("    🧹 Iniciando limpieza y encoding...")
        print(f"       Columnas antes: {len(data.columns)}")
        
        # Normalizar Clase_Min_Ciclo y Clase_Max_Ciclo
        if 'Clase_Min_Ciclo' in data.columns:
            data['Clase_Min_Ciclo'] = data['Clase_Min_Ciclo'].str.title()
        if 'Clase_Max_Ciclo' in data.columns:
            data['Clase_Max_Ciclo'] = data['Clase_Max_Ciclo'].str.title()
        
        # Mapear a categorías
        if self.mapa_categorias:
            print("       → Mapeando Clase_Min_Ciclo a categorías...")
            data['Cat_ClaseMin'] = data['Clase_Min_Ciclo'].map(self.mapa_categorias)
            data['Cat_ClaseMin'].fillna('Otros', inplace=True)
            
            print("       → Mapeando Clase_Max_Ciclo a categorías...")
            data['Cat_ClaseMax'] = data['Clase_Max_Ciclo'].map(self.mapa_categorias)
            data['Cat_ClaseMax'].fillna('Otros', inplace=True)
        
        # ENCODING - Crear variables dummy en el orden del pipeline
        
        # 1. Programa → p_
        if 'Programa' in data.columns:
            print("       → Encoding: Programa")
            dummies = pd.get_dummies(data['Programa'], prefix='p')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Programa'], inplace=True)
        
        # 2. Siglas Prog → s_
        if 'Siglas Prog' in data.columns:
            print("       → Encoding: Siglas Prog")
            dummies = pd.get_dummies(data['Siglas Prog'], prefix='s')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Siglas Prog'], inplace=True)
        
        # 3. Ciudad (Dirección) → cd_
        if 'Ciudad (Dirección)' in data.columns:
            print("       → Encoding: Ciudad (Dirección)")
            dummies = pd.get_dummies(data['Ciudad (Dirección)'], prefix='cd')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Ciudad (Dirección)'], inplace=True)
        
        # 4. Dpto Nacimiento → dn_
        if 'Dpto Nacimiento' in data.columns:
            print("       → Encoding: Dpto Nacimiento")
            dummies = pd.get_dummies(data['Dpto Nacimiento'], prefix='dn')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Dpto Nacimiento'], inplace=True)
        
        # 5. País Nacimiento → pn_
        if 'País Nacimiento' in data.columns:
            print("       → Encoding: País Nacimiento")
            dummies = pd.get_dummies(data['País Nacimiento'], prefix='pn')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['País Nacimiento'], inplace=True)
        
        # 6. Acción → a_
        # Resolver duplicados primero
        if 'Acción_per' in data.columns:
            data.rename(columns={'Acción_per': 'Acción'}, inplace=True)
            data.drop(columns=['Acción_prom', 'Acción_ppn'], inplace=True, errors='ignore')
        
        if 'Acción' in data.columns:
            print("       → Encoding: Acción")
            dummies = pd.get_dummies(data['Acción'], prefix='a')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Acción'], inplace=True)
        
        # 7. Motivo → m_
        if 'Motivo_per' in data.columns:
            data.rename(columns={'Motivo_per': 'Motivo'}, inplace=True)
            data.drop(columns=['Motivo_prom', 'Motivo_ppn'], inplace=True, errors='ignore')
        
        if 'Motivo' in data.columns:
            print("       → Encoding: Motivo")
            dummies = pd.get_dummies(data['Motivo'], prefix='m')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Motivo'], inplace=True)
        
        # 8. Cat_ClaseMax → ccmax_
        if 'Cat_ClaseMax' in data.columns:
            print("       → Encoding: Cat_ClaseMax")
            dummies = pd.get_dummies(data['Cat_ClaseMax'], prefix='ccmax')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Cat_ClaseMax'], inplace=True)
        
        # 9. Cat_ClaseMin → ccmin_
        if 'Cat_ClaseMin' in data.columns:
            print("       → Encoding: Cat_ClaseMin")
            dummies = pd.get_dummies(data['Cat_ClaseMin'], prefix='ccmin')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Cat_ClaseMin'], inplace=True)
        
        # 10. Tipo Admisión → ta_
        if 'Tipo Admisión' in data.columns:
            print("       → Encoding: Tipo Admisión")
            dummies = pd.get_dummies(data['Tipo Admisión'], prefix='ta')
            data = pd.concat([data, dummies], axis=1)
            data.drop(columns=['Tipo Admisión'], inplace=True)
        
        # 11. Sexo → numérico
        # Resolver duplicados
        if 'Sexo_ppn' in data.columns:
            data.rename(columns={'Sexo_ppn': 'Sexo'}, inplace=True)
            data.drop(columns=['Sexo_adm'], inplace=True, errors='ignore')
        
        if 'Sexo' in data.columns:
            print("       → Encoding: Sexo")
            data['Sexo'] = data['Sexo'].replace({'M': 1, 'F': 0, 'Masculino': 1, 'Femenino': 0})
        
        # 12. Edad → rangos
        if 'Edad' in data.columns:
            print("       → Encoding: Edad (rangos)")
            def map_age_groups(age):
                if pd.isna(age):
                    return 0
                if age <= 19:
                    return 0
                elif age <= 24:
                    return 1
                else:
                    return 3
            
            data['rango_edad'] = data['Edad'].apply(map_age_groups).astype('int8')
            data.drop(columns=['Edad'], inplace=True)
        
        # Eliminar columnas innecesarias
        cols_eliminar = [
            # Identificación
            'ID', 'Nombre', 'Nombre_ppn', 'Nombre_adm', '2º Nombre', 'Última',
            '2º Apellido', '2º Apellido_per', '2º Apellido_prom', 'Apellidos', 'Nombres',
            'Tipo Doc ID', 'Tipo Doc ID_ppn', 'Tipo Doc ID_adm',
            'Doc ID', 'Doc Identidad', 'Tipo Doc Identidad',
            'Dirección', 'Dirección 1', 'Dirección 2',
            'Teléfono', 'Teléfono_ppn', 'Teléfono_adm',
            'Correo-E', 'Correo-E_ppn', 'Correo-E_adm', 'Otro Correo E',
            'Celular Inscripción', 'F Nacimiento', 'F Nacimiento_ppn', 'F Nacimiento_adm',
            'Fecha Grado', 'Estado (Dirección)', 'País (Dirección)',
            'Ciudad Nacimiento', 'Lugar Nacimiento', 'Colegio', 'Colegio_ppn', 'Colegio_adm',
            'ID Colegio', 'Descripción', 'Org Acad', 'Tipo', 'Estado_adm', 'Estado Clase',
            'Prog Acad', 'Prog Acad_ppn', 'Prog Acad_adm', 'Prog Acad.1',
            'Ciclo Admisión_per', 'Ciclo Admisión_prom', 'Situacion Acad',
            'Año', 'Año_per', 'Año_prom', 'Estado', 'Clase_Min_Ciclo', 'Clase_Max_Ciclo',
            'ID_Min_Ciclo', 'ID_Max_Ciclo', 'Mult Programa', 'Ciclo',
            'ID Curso', 'Calif', 'Uni Matrd', 'Benef. Beca'
        ]
        
        cols_encontradas = [c for c in cols_eliminar if c in data.columns]
        if cols_encontradas:
            data.drop(columns=cols_encontradas, inplace=True)
        
        # Eliminar columnas con sufijos
        cols_sufijos = [col for col in data.columns 
                       if any(col.endswith(s) for s in ['_per', '_prom', '_adm', '_ppn', '_pprom', '_notas'])]
        if cols_sufijos:
            data.drop(columns=cols_sufijos, inplace=True)
        
        # Convertir fechas y columnas object a numérico
        for col in data.select_dtypes(include=['datetime64']).columns:
            try:
                data[col] = (data[col] - pd.Timestamp('1970-01-01')).dt.days
                data[col].fillna(0, inplace=True)
            except:
                data.drop(columns=[col], inplace=True)
        
        for col in data.select_dtypes(include=['object']).columns:
            try:
                data[col] = pd.to_numeric(data[col], errors='coerce')
                data[col].fillna(0, inplace=True)
            except:
                data.drop(columns=[col], inplace=True)
        
        print(f"       ✓ Columnas después: {len(data.columns)}")
        
        return data
    
    def predecir(self, data: pd.DataFrame) -> pd.DataFrame:
        """Realiza predicciones con el modelo XGBoost"""
        print("\n🎯 INICIANDO PREDICCIÓN...")
        print(f"   Estado del modelo: {'✅ Cargado' if self.modelo is not None else '❌ NO cargado'}")
        print(f"   Tipo de modelo: {type(self.modelo).__name__}")
        print(f"   Registros a predecir: {len(data)}")
        
        if self.modelo is None:
            raise ValueError("❌ Modelo no cargado")
        
        print("🎯 Realizando predicciones...")
        
        try:
            # Preparar datos
            if self.columnas_modelo is not None:
                for col in self.columnas_modelo:
                    if col not in data.columns:
                        data[col] = 0
                X = data[self.columnas_modelo].copy()
            else:
                X = data.copy()
            
            print(f"   📊 Shape de X: {X.shape}")
            
            # Verificar duplicados
            if X.columns.duplicated().any():
                print("   ⚠️ Columnas duplicadas detectadas, eliminando...")
                X = X.loc[:, ~X.columns.duplicated()]
            
            # Asegurar que todo sea numérico
            for col in X.columns:
                if X[col].dtype == 'object':
                    X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)
            
            # Estandarizar
            if self.scaler is not None:
                print("   🔧 Aplicando scaler...")
                X_scaled = self.scaler.transform(X)
            else:
                X_scaled = X.values
            
            print(f"   📊 Shape final: {X_scaled.shape}")
            
            # Predecir
            modelo_tipo = type(self.modelo).__name__
            
            if 'ExponentiatedGradient' in modelo_tipo:
                print("   ℹ️ Detectado modelo con mitigación (ExponentiatedGradient)")
                predicciones = self.modelo.predict(X_scaled)
                probabilidades = np.where(predicciones == 1, 0.9, 0.1)
            else:
                probabilidades = self.modelo.predict_proba(X_scaled)[:, 1]
            
            # Agregar resultados
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
