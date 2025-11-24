"""
Data Processor XGBoost - Con método predecir_procesado integrado
"""

import pandas as pd
import numpy as np
import os
import joblib
from typing import Dict, Tuple

class DataProcessorXGBoost:
    """
    Procesador que carga y ejecuta el modelo XGBoost
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
    
    def predecir(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza predicciones con el modelo XGBoost
        NOTA: Este método asume que los datos ya fueron procesados
        """
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
    
  def predecir_procesado(self, data_procesada: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza predicciones con datos YA PROCESADOS por el pipeline integrado
        
        CORRECCIÓN CRÍTICA: Calcula probabilidades correctamente para modelos
        ExponentiatedGradient con múltiples predictores y pesos.
        
        Args:
            data_procesada: DataFrame ya procesado (limpieza + encoding + ajustes)
            
        Returns:
            DataFrame con columnas adicionales:
            - probabilidad: Probabilidad de deserción (0-1)
            - nivel_riesgo: Nivel de riesgo ("Bajo", "Medio", "Alto")
        """
        print("\n" + "="*80)
        print("🎯 INICIANDO PREDICCIÓN CON DATOS PROCESADOS")
        print("="*80)
        print(f"   📊 Registros recibidos: {len(data_procesada):,}")
        print(f"   📊 Columnas recibidas: {len(data_procesada.columns)}")
        
        # Verificar que el modelo esté cargado
        if self.modelo is None:
            raise ValueError(
                "❌ Modelo XGBoost no cargado.\n"
                "   Verifica que 'xgboost_modelo.pkl' esté en la raíz del proyecto."
            )
        
        print(f"   ✅ Modelo cargado: {type(self.modelo).__name__}")
        
        try:
            # ============================================================
            # PREPARACIÓN DE DATOS
            # ============================================================
            
            X = data_procesada.copy()
            
            print("\n🔧 Preparando datos para predicción...")
            
            # 1. Eliminar columnas problemáticas PRIMERO
            columnas_a_eliminar = []
            
            # Buscar desercion/deserción
            for col in X.columns:
                col_lower = col.lower()
                if 'desercion' in col_lower or 'deserción' in col_lower:
                    columnas_a_eliminar.append(col)
            
            # Buscar Estado (Dropout)
            for col in X.columns:
                if col == 'Estado (Dropout)' or col == 'Estado_Dropout':
                    columnas_a_eliminar.append(col)
            
            # Eliminar columnas identificadoras
            cols_id = ['ID', 'Mult Programa', 'Ciclo']
            for col in cols_id:
                if col in X.columns:
                    columnas_a_eliminar.append(col)
            
            if columnas_a_eliminar:
                X = X.drop(columns=columnas_a_eliminar, errors='ignore')
                print(f"   ✓ {len(columnas_a_eliminar)} columnas eliminadas")
            
            # 2. Eliminar columnas no numéricas
            cols_object = X.select_dtypes(include=['object']).columns.tolist()
            if cols_object:
                X = X.drop(columns=cols_object)
            
            # 3. Eliminar columnas duplicadas
            if X.columns.duplicated().any():
                X = X.loc[:, ~X.columns.duplicated()]
            
            # 4. Manejar valores infinitos y NaN
            X = X.replace([np.inf, -np.inf], np.nan)
            if X.isnull().any().any():
                X = X.fillna(0)
            
            # 5. Alinear con columnas del modelo
            if self.columnas_modelo is not None:
                print(f"   → Alineando con {len(self.columnas_modelo)} columnas del modelo")
                
                # Agregar columnas faltantes con 0
                for col in self.columnas_modelo:
                    if col not in X.columns:
                        X[col] = 0
                
                # Ordenar columnas según el orden del entrenamiento (CRÍTICO)
                X = X[self.columnas_modelo]
            
            print(f"   ✅ Datos preparados: {X.shape}")
            
            # ============================================================
            # APLICAR SCALER (CRÍTICO)
            # ============================================================
            
            if self.scaler is not None:
                print("\n   🔧 Aplicando scaler (estandarización)...")
                X_scaled = self.scaler.transform(X)
                print(f"      ✓ Datos escalados: {X_scaled.shape}")
            else:
                print("\n   ⚠️ NO HAY SCALER - Esto puede causar predicciones incorrectas")
                X_scaled = X.values
            
            # ============================================================
            # PREDICCIÓN CON CÁLCULO CORRECTO DE PROBABILIDADES
            # ============================================================
            
            print("\n🤖 Ejecutando predicción con XGBoost...")
            
            modelo_tipo = type(self.modelo).__name__
            print(f"   Tipo de modelo: {modelo_tipo}")
            
            # CORRECCIÓN CRÍTICA: Calcular probabilidades correctamente
            if 'ExponentiatedGradient' in modelo_tipo:
                print("   ℹ️ Modelo con mitigación de sesgo detectado")
                
                # Predicciones de clase
                predicciones = self.modelo.predict(X_scaled)
                
                # CALCULAR PROBABILIDADES DESDE PREDICTORES INTERNOS
                if hasattr(self.modelo, 'predictors_') and hasattr(self.modelo, 'weights_'):
                    print(f"   🔍 Calculando probabilidades desde {len(self.modelo.predictors_)} predictores")
                    
                    probabilidades_lista = []
                    
                    # Obtener probabilidades de cada predictor
                    for i, predictor in enumerate(self.modelo.predictors_):
                        proba = predictor.predict_proba(X_scaled)[:, 1]
                        probabilidades_lista.append(proba)
                    
                    # Promedio ponderado con los pesos del modelo
                    probabilidades = np.average(
                        probabilidades_lista,
                        axis=0,
                        weights=self.modelo.weights_
                    )
                    
                    print(f"   ✅ Probabilidades calculadas con promedio ponderado")
                    print(f"      Pesos del modelo: {self.modelo.weights_[:5]}..." if len(self.modelo.weights_) > 5 else f"      Pesos: {self.modelo.weights_}")
                    
                else:
                    # Fallback: usar predicciones como probabilidades
                    print("   ⚠️ No se encontraron predictores internos, usando predicciones directas")
                    probabilidades = predicciones.astype(float)
            
            else:
                # Modelo estándar (sin mitigación)
                if hasattr(self.modelo, 'predict_proba'):
                    probabilidades = self.modelo.predict_proba(X_scaled)[:, 1]
                else:
                    predicciones = self.modelo.predict(X_scaled)
                    probabilidades = predicciones.astype(float)
            
            print(f"   ✅ Predicciones generadas: {len(probabilidades):,}")
            
            # ============================================================
            # VALIDAR PROBABILIDADES
            # ============================================================
            
            print("\n🔍 Validando probabilidades...")
            print(f"   Rango: [{probabilidades.min():.4f}, {probabilidades.max():.4f}]")
            print(f"   Media: {probabilidades.mean():.4f}")
            print(f"   Mediana: {np.median(probabilidades):.4f}")
            print(f"   Std: {probabilidades.std():.4f}")
            
            # Verificar si todas son iguales (problema detectado)
            valores_unicos = np.unique(probabilidades)
            if len(valores_unicos) == 1:
                print(f"   ⚠️ WARNING: Todas las probabilidades son {valores_unicos[0]:.4f}")
                print(f"   Esto indica un problema en el cálculo o datos")
            elif len(valores_unicos) < 10:
                print(f"   ⚠️ WARNING: Solo {len(valores_unicos)} valores únicos de probabilidad")
                print(f"   Valores: {valores_unicos}")
            else:
                print(f"   ✅ {len(valores_unicos):,} valores únicos de probabilidad (correcto)")
            
            # ============================================================
            # AGREGAR RESULTADOS AL DATAFRAME
            # ============================================================
            
            resultado = data_procesada.copy()
            resultado['probabilidad'] = probabilidades
            
            # Clasificar nivel de riesgo
            resultado['nivel_riesgo'] = pd.cut(
                probabilidades,
                bins=[0, 0.3, 0.6, 1.0],
                labels=["Bajo", "Medio", "Alto"]
            )
            
            # ============================================================
            # ESTADÍSTICAS FINALES
            # ============================================================
            
            print("\n" + "="*80)
            print("✅ PREDICCIÓN COMPLETADA")
            print("="*80)
            print(f"   📊 Estudiantes analizados: {len(resultado):,}")
            print(f"   📊 Probabilidad promedio: {probabilidades.mean():.2%}")
            print(f"   📊 Probabilidad mínima: {probabilidades.min():.2%}")
            print(f"   📊 Probabilidad máxima: {probabilidades.max():.2%}")
            print(f"   📊 Desviación estándar: {probabilidades.std():.4f}")
            print("\n   📈 Distribución de riesgo:")
            print(f"      🟢 Bajo (<30%):   {(resultado['nivel_riesgo']=='Bajo').sum():>6,} estudiantes ({(resultado['nivel_riesgo']=='Bajo').sum()/len(resultado)*100:>5.1f}%)")
            print(f"      🟡 Medio (30-60%): {(resultado['nivel_riesgo']=='Medio').sum():>6,} estudiantes ({(resultado['nivel_riesgo']=='Medio').sum()/len(resultado)*100:>5.1f}%)")
            print(f"      🔴 Alto (>60%):    {(resultado['nivel_riesgo']=='Alto').sum():>6,} estudiantes ({(resultado['nivel_riesgo']=='Alto').sum()/len(resultado)*100:>5.1f}%)")
            print("="*80 + "\n")
            
            return resultado
            
        except Exception as e:
            print(f"\n❌ ERROR EN PREDICCIÓN: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    
    def get_summary_stats(self, df: pd.DataFrame) -> dict:
        """Genera estadísticas resumidas del dataframe procesado"""
        stats = {
            'total_estudiantes': len(df),
            'riesgo_bajo': (df['nivel_riesgo'] == 'Bajo').sum() if 'nivel_riesgo' in df.columns else 0,
            'riesgo_medio': (df['nivel_riesgo'] == 'Medio').sum() if 'nivel_riesgo' in df.columns else 0,
            'riesgo_alto': (df['nivel_riesgo'] == 'Alto').sum() if 'nivel_riesgo' in df.columns else 0,
        }
        return stats
