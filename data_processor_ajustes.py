"""
Data Processor AJUSTES FINALES - Dropout corrida y preparación para predicción
Replica el proceso de ajuste de dropout y preparación final de columnas
Recibe base codificada y retorna base lista para el modelo
"""

import pandas as pd
import numpy as np
from typing import Optional

class DataProcessorAjustes:
    """
    Procesador que aplica ajustes finales:
    - Elimina programas finalizados
    - Calcula dropout corrida (deserción en próximo ciclo)
    - Limpia y prepara columnas finales
    - Ordena columnas según modelo
    """
    
    def __init__(self, per_original: Optional[pd.DataFrame] = None,
                 columnas_path: Optional[str] = None):
        """
        Inicializa el procesador de ajustes
        
        Args:
            per_original: DataFrame PER original (para validar continuidad)
            columnas_path: Ruta al archivo CSV con columnas finales del modelo
        """
        self.per_original = per_original
        self.columnas_modelo = None
        
        if columnas_path:
            self._cargar_columnas_modelo(columnas_path)
        
        print("✅ Procesador de Ajustes Finales inicializado")
    
    def _cargar_columnas_modelo(self, path: str):
        """Carga las columnas del modelo desde CSV"""
        try:
            columnas_df = pd.read_csv(path)
            self.columnas_modelo = columnas_df.columns.tolist()
            print(f"   ✓ Columnas del modelo cargadas: {len(self.columnas_modelo)} columnas")
        except Exception as e:
            print(f"   ⚠️ Error al cargar columnas del modelo: {e}")
            self.columnas_modelo = None
    
    def procesar(self, data_encoded: pd.DataFrame, 
                 per_original: Optional[pd.DataFrame] = None,
                 columnas_modelo_path: Optional[str] = None) -> pd.DataFrame:
        """
        Procesa la base codificada y aplica ajustes finales
        
        Args:
            data_encoded: DataFrame resultado del encoding
            per_original: DataFrame PER original (opcional)
            columnas_modelo_path: Ruta al Excel con columnas (opcional)
            
        Returns:
            DataFrame listo para predicción
        """
        print("\n" + "="*80)
        print("🔧 INICIANDO AJUSTES FINALES - DROPOUT CORRIDA")
        print("="*80)
        
        # Actualizar PER si se proporciona
        if per_original is not None:
            self.per_original = per_original
        
        # Cargar columnas si se proporciona
        if columnas_modelo_path and not self.columnas_modelo:
            self._cargar_columnas_modelo(columnas_modelo_path)
        
        data = data_encoded.copy()
        
        # ========== FASE 1: ELIMINAR PROGRAMAS FINALIZADOS ==========
        print("\n" + "="*80)
        print("FASE 1: ELIMINAR PROGRAMAS FINALIZADOS")
        print("="*80)
        
        data = self._eliminar_programas_finalizados(data)
        
        # ========== FASE 2: CALCULAR DROPOUT CORRIDA ==========
        print("\n" + "="*80)
        print("FASE 2: CALCULAR DROPOUT CORRIDA")
        print("="*80)
        
        data = self._calcular_dropout_corrida(data)
        
        # ========== FASE 3: AJUSTAR ÚLTIMO CICLO CON DESERCIÓN ==========
        print("\n" + "="*80)
        print("FASE 3: AJUSTAR DROPOUT EN ÚLTIMO CICLO")
        print("="*80)
        
        data = self._ajustar_dropout_ultimo_ciclo(data)
        
        # ========== FASE 4: DETECTAR PAUSAS LARGAS ==========
        print("\n" + "="*80)
        print("FASE 4: DETECTAR PAUSAS LARGAS (≥3 SEMESTRES)")
        print("="*80)
        
        data = self._detectar_pausas_largas(data)
        
        # ========== FASE 5: CREAR ESTADO_NEXT ==========
        print("\n" + "="*80)
        print("FASE 5: CREAR ESTADO_NEXT (PREDICCIÓN)")
        print("="*80)
        
        data = self._crear_estado_next(data)
        
        # ========== FASE 6: VALIDAR CON PER ORIGINAL ==========
        print("\n" + "="*80)
        print("FASE 6: VALIDAR CONTINUIDAD CON PER")
        print("="*80)
        
        data = self._validar_con_per(data)
        
        # ========== FASE 7: RENOMBRAR Y LIMPIAR COLUMNAS ==========
        print("\n" + "="*80)
        print("FASE 7: RENOMBRAR Y LIMPIAR COLUMNAS")
        print("="*80)
        
        data = self._renombrar_y_limpiar_columnas(data)
        
        # ========== FASE 8: CONVERTIR TIPOS DE DATOS ==========
        print("\n" + "="*80)
        print("FASE 8: CONVERTIR TIPOS DE DATOS")
        print("="*80)
        
        data = self._convertir_tipos_datos(data)
        
        # ========== FASE 9: ELIMINAR REGISTROS SIN ESTADO_NEXT ==========
        print("\n" + "="*80)
        print("FASE 9: FILTRAR REGISTROS VÁLIDOS")
        print("="*80)
        
        data = self._filtrar_registros_validos(data)
        
        # ========== FASE 10: CREAR RANGO EDAD ==========
        print("\n" + "="*80)
        print("FASE 10: CREAR RANGO DE EDAD")
        print("="*80)
        
        data = self._crear_rango_edad(data)
        
        # ========== FASE 11: APLICAR COLUMNAS DEL MODELO ==========
        print("\n" + "="*80)
        print("FASE 11: APLICAR COLUMNAS DEL MODELO")
        print("="*80)
        
        data = self._aplicar_columnas_modelo(data)
        
        # ========== FASE 12: ELIMINAR DUPLICADOS ==========
        print("\n" + "="*80)
        print("FASE 12: ELIMINAR COLUMNAS DUPLICADAS")
        print("="*80)
        
        data = self._eliminar_duplicados(data)
        
        print("\n" + "="*80)
        print(f"✅ AJUSTES COMPLETADOS")
        print(f"   • Registros finales: {len(data)}")
        print(f"   • Columnas finales: {len(data.columns)}")
        print(f"   • Lista para predicción")
        print("="*80)
        
        return data
    
    # ============================================================================
    # FASE 1: ELIMINAR PROGRAMAS FINALIZADOS
    # ============================================================================
    
    def _eliminar_programas_finalizados(self, data):
        """Eliminar registros con Estado = 'Programa Finalizado'"""
        print("\n🗑️ Eliminando programas finalizados...")
        
        if 'Estado' not in data.columns:
            print("   ⚠️ Columna 'Estado' no encontrada")
            return data
        
        registros_antes = len(data)
        data = data[data['Estado'] != 'Programa Finalizado'].copy()
        registros_despues = len(data)
        
        print(f"   ✓ Registros: {registros_antes} → {registros_despues} ({registros_antes - registros_despues} eliminados)")
        
        return data
    
    # ============================================================================
    # FASE 2: CALCULAR DROPOUT CORRIDA
    # ============================================================================
    
    def _calcular_dropout_corrida(self, data):
        """Si último ciclo tiene Dropout=1, poner 0 a todos los anteriores"""
        print("\n🔄 Calculando dropout corrida...")
        
        if 'Estado (Dropout)' not in data.columns:
            print("   ⚠️ Columna 'Estado (Dropout)' no encontrada")
            return data
        
        # Identificar columnas p_
        cols_p = [col for col in data.columns if col.startswith('p_')]
        print(f"   ✓ Programas encontrados: {len(cols_p)}")
        
        # Ordenar por ID, Mult Programa y Ciclo
        data = data.sort_values(by=['ID', 'Mult Programa', 'Ciclo'])
        
        ajustes_count = 0
        
        for col in cols_p:
            mask_true = data[col] == 1
            sub_data = data[mask_true]
            grupos = sub_data.groupby(['ID', 'Mult Programa'])
            
            for (id_val, mp_val), group in grupos:
                idx_max = group['Ciclo'].idxmax()
                estado_final = data.loc[idx_max, 'Estado (Dropout)']
                
                if estado_final == 1:
                    idx_anteriores = group[group['Ciclo'] < data.loc[idx_max, 'Ciclo']].index
                    data.loc[idx_anteriores, 'Estado (Dropout)'] = 0
                    ajustes_count += len(idx_anteriores)
        
        print(f"   ✓ Ajustes realizados: {ajustes_count} registros")
        
        return data
    
    # ============================================================================
    # FASE 3: AJUSTAR DROPOUT EN ÚLTIMO CICLO
    # ============================================================================
    
    def _ajustar_dropout_ultimo_ciclo(self, data):
        """Ajustar dropout en el último ciclo"""
        print("\n🔧 Ajustando dropout en último ciclo...")
        
        # Esta fase ya está implícita en la anterior
        print("   ✓ Ya aplicado en fase anterior")
        
        return data
    
    # ============================================================================
    # FASE 4: DETECTAR PAUSAS LARGAS
    # ============================================================================
    
    def _detectar_pausas_largas(self, data):
        """Detectar pausas de ≥3 semestres y marcar como deserción"""
        print("\n⏸️ Detectando pausas largas (≥3 semestres)...")
        
        ciclos_ref = [
            510, 530, 610, 630, 710, 730, 810, 830, 910, 930, 1010, 1030, 1110, 1130,
            1210, 1230, 1310, 1330, 1410, 1430, 1510, 1530, 1610, 1630, 1710, 1730,
            1810, 1830, 1910, 1930, 2010, 2030, 2110, 2130, 2210, 2230, 2310, 2330,
            2410, 2430, 2510, 2530, 2610, 2630, 2710, 2730, 2810, 2830, 2910, 2930, 3010, 3030
        ]
        
        cols_p = [col for col in data.columns if col.startswith('p_')]
        data = data.sort_values(by=['ID', 'Mult Programa', 'Ciclo'])
        
        pausas_detectadas = 0
        
        for col in cols_p:
            sub_data = data[data[col] == 1]
            grupos = sub_data.groupby(['ID', 'Mult Programa'])
            
            for (id_val, mp_val), group in grupos:
                ciclos_estudiante = sorted(group['Ciclo'].unique())
                
                for i in range(len(ciclos_estudiante) - 1):
                    ciclo_actual = ciclos_estudiante[i]
                    ciclo_siguiente = ciclos_estudiante[i + 1]
                    
                    if ciclo_actual not in ciclos_ref or ciclo_siguiente not in ciclos_ref:
                        continue
                    
                    idx_actual = ciclos_ref.index(ciclo_actual)
                    idx_siguiente = ciclos_ref.index(ciclo_siguiente)
                    diferencia = idx_siguiente - idx_actual
                    
                    if diferencia >= 3:
                        idx_mod = data[
                            (data['ID'] == id_val) &
                            (data['Mult Programa'] == mp_val) &
                            (data['Ciclo'] == ciclo_actual) &
                            (data[col] == 1)
                        ].index
                        data.loc[idx_mod, 'Estado (Dropout)'] = 1
                        pausas_detectadas += len(idx_mod)
        
        print(f"   ✓ Pausas detectadas: {pausas_detectadas} registros marcados")
        
        return data
    
    # ============================================================================
    # FASE 5: CREAR ESTADO_NEXT
    # ============================================================================
    
    def _crear_estado_next(self, data):
        """Crear variable Estado_next (predicción del próximo ciclo)"""
        print("\n🎯 Creando Estado_next...")
        
        cols_p = [c for c in data.columns if c.startswith('p_')]
        data['Estado_next'] = 0
        data = data.sort_values(['ID', 'Mult Programa', 'Ciclo']).reset_index(drop=True)
        
        for col_p in cols_p:
            sub = data[data[col_p] == 1]
            for (id_val, mp_val), group in sub.groupby(['ID', 'Mult Programa']):
                ciclos_grupo = group['Ciclo'].to_list()
                
                if len(ciclos_grupo) == 0:
                    continue
                
                # 1. Dos ciclos de mayor valor → NaN
                top2_cycles = sorted(ciclos_grupo)[-2:] if len(ciclos_grupo) >= 2 else [max(ciclos_grupo)]
                for ciclo_top in top2_cycles:
                    idx_top = group[group['Ciclo'] == ciclo_top].index[0]
                    data.loc[idx_top, 'Estado_next'] = np.nan
                
                max_cycle = max(top2_cycles)
                
                # 2. Si último tiene Dropout=1, marcar dos ciclos antes
                if data.loc[idx_top, 'Estado (Dropout)'] == 1:
                    ciclos_ordenados = sorted(ciclos_grupo)
                    pos_desercion = ciclos_ordenados.index(max_cycle)
                    
                    if pos_desercion >= 2:
                        ciclo_target = ciclos_ordenados[pos_desercion - 2]
                        idx_target = group[group['Ciclo'] == ciclo_target].index[0]
                        data.loc[idx_target, 'Estado_next'] = 1
                    else:
                        anteriores = [c for c in ciclos_grupo if c < max_cycle]
                        if len(anteriores) > 0:
                            ciclo_target = max(anteriores)
                            idx_prev = group[group['Ciclo'] == ciclo_target].index[0]
                            data.loc[idx_prev, 'Estado_next'] = 1
                
                # 3. Pausas en ciclos menores
                for i, ciclo in enumerate(ciclos_grupo):
                    idx = group[group['Ciclo'] == ciclo].index[0]
                    if ciclo < max_cycle and data.loc[idx, 'Estado (Dropout)'] == 1:
                        data.loc[idx, 'Estado_next'] = 1
        
        # Marcar penúltimo ciclo como 0
        data["Ciclo"] = data["Ciclo"].astype(int)
        ciclos_unicos = sorted(data['Ciclo'].unique())
        
        if len(ciclos_unicos) >= 2:
            penultimo_ciclo = ciclos_unicos[-2]
            data.loc[data['Ciclo'] == penultimo_ciclo, 'Estado_next'] = 0
            print(f"   ✓ Penúltimo ciclo ({penultimo_ciclo}) marcado como 0")
        
        print(f"   ✓ Estado_next creado")
        
        return data
    
    # ============================================================================
    # FASE 6: VALIDAR CON PER
    # ============================================================================
    
    def _validar_con_per(self, data):
        """Validar continuidad con PER original"""
        print("\n✅ Validando con PER original...")
        
        if self.per_original is None:
            print("   ⚠️ PER original no proporcionado, saltando validación")
            return data
        
        # Crear columna Programa a partir de p_
        data['Programa'] = data[[c for c in data.columns if c.startswith('p_')]].idxmax(axis=1).str.replace('p_', '')
        
        ciclos_unicos = sorted(data['Ciclo'].unique())
        if len(ciclos_unicos) < 2:
            print("   ⚠️ No hay suficientes ciclos para validación")
            return data
        
        ciclo_penultimo = ciclos_unicos[-2]
        ciclo_max_per = self.per_original['Ciclo'].max()
        
        # IDs en ciclo máximo de PER
        ids_max_per_prog = set(zip(
            self.per_original.loc[self.per_original['Ciclo'] == ciclo_max_per, 'ID'],
            self.per_original.loc[self.per_original['Ciclo'] == ciclo_max_per, 'Programa']
        ))
        
        # IDs en penúltimo ciclo de data
        ids_penultimo_data = set(zip(
            data.loc[data['Ciclo'] == ciclo_penultimo, 'ID'],
            data.loc[data['Ciclo'] == ciclo_penultimo, 'Programa']
        ))
        
        # Los que NO están en PER son dropout
        ids_no_en_per = ids_penultimo_data - ids_max_per_prog
        
        data.loc[data['Ciclo'] == ciclo_penultimo, 'Estado_next'] = data.loc[data['Ciclo'] == ciclo_penultimo].apply(
            lambda row: 1 if (row['ID'], row['Programa']) in ids_no_en_per else np.nan,
            axis=1
        )
        
        print(f"   ✓ Validación completada: {len(ids_no_en_per)} deserciones detectadas")
        
        return data
    
    # ============================================================================
    # FASE 7: RENOMBRAR Y LIMPIAR COLUMNAS
    # ============================================================================
    
    def _renombrar_y_limpiar_columnas(self, data):
        """Renombrar y eliminar columnas innecesarias"""
        print("\n📝 Renombrando y limpiando columnas...")
        
        # Renombres
        renombres = {
            'Créditos Inscritos en Ciclo_prom': 'Créditos Inscritos en Ciclo',
            'Créd.Inscritos y Aprobados Ciclo_prom': 'Créditos Inscritos y Aprobados Ciclo'
        }
        
        data = data.rename(columns={k: v for k, v in renombres.items() if k in data.columns})
        
        # Eliminar columnas
        columnas_eliminar = [
            'Créditos Inscritos en Ciclo_per',
            'Créd.Inscritos y Aprobados Ciclo_per',
            'Situacion Acad',
            'Créd Inscritos xa PromedioCicl',
            'Créd.Inscrtos Aprbdos PromCicl',
            'Num_Materias_Ciclo'
        ]
        
        cols_found = [c for c in columnas_eliminar if c in data.columns]
        if cols_found:
            data = data.drop(columns=cols_found)
            print(f"   ✓ Eliminadas: {len(cols_found)} columnas")
        
        # Eliminar pn_*
        cols_pn = [c for c in data.columns if c.startswith('pn_')]
        if cols_pn:
            data = data.drop(columns=cols_pn)
            print(f"   ✓ Eliminadas {len(cols_pn)} columnas pn_*")
        
        return data
    
    # ============================================================================
    # FASE 8: CONVERTIR TIPOS DE DATOS
    # ============================================================================
    
    def _convertir_tipos_datos(self, data):
        """Convertir tipos de datos a formatos eficientes"""
        print("\n🔢 Convirtiendo tipos de datos...")
        
        # Convertir bool a int8
        bool_cols = data.select_dtypes(include=['bool']).columns
        if len(bool_cols) > 0:
            data = data.astype({col: 'int8' for col in bool_cols})
            print(f"   ✓ {len(bool_cols)} columnas bool → int8")
        
        # Convertir Ciclo a Int64
        if 'Ciclo' in data.columns:
            data['Ciclo'] = pd.to_numeric(data['Ciclo'], errors='coerce').astype('Int64')
        
        if 'Ciclo Admisión' in data.columns:
            data['Ciclo Admisión'] = pd.to_numeric(data['Ciclo Admisión'], errors='coerce').astype('Int64')
        
        # Eliminar columnas object (excepto ID)
        object_cols = data.select_dtypes(include=['object']).columns.tolist()
        object_cols = [c for c in object_cols if c != 'ID']
        
        if object_cols:
            data = data.drop(columns=object_cols)
            print(f"   ✓ Eliminadas {len(object_cols)} columnas object")
        
        # Eliminar ID y Estado (Dropout)
        cols_final_drop = ['ID', 'Estado (Dropout)', 'Estado', 'Programa']
        cols_final_found = [c for c in cols_final_drop if c in data.columns]
        if cols_final_found:
            data = data.drop(columns=cols_final_found)
            print(f"   ✓ Eliminadas: {cols_final_found}")
        
        return data
    
    # ============================================================================
    # FASE 9: FILTRAR REGISTROS VÁLIDOS
    # ============================================================================
    
    def _filtrar_registros_validos(self, data):
        """Filtrar solo registros con Estado_next válido"""
        print("\n🔍 Filtrando registros válidos...")
        
        if 'Estado_next' not in data.columns:
            print("   ⚠️ Columna 'Estado_next' no encontrada")
            return data
        
        registros_antes = len(data)
        data = data[~data['Estado_next'].isna()].copy()
        registros_despues = len(data)
        
        # Renombrar a desercion
        data = data.rename(columns={"Estado_next": "desercion"})
        
        print(f"   ✓ Registros: {registros_antes} → {registros_despues} ({registros_antes - registros_despues} eliminados)")
        
        return data
    
    # ============================================================================
    # FASE 10: CREAR RANGO EDAD
    # ============================================================================
    
    def _crear_rango_edad(self, data):
        """Crear variable rango_edad categórica y eliminar Edad"""
        print("\n👥 Creando rango de edad...")
        
        if 'Edad' not in data.columns:
            print("   ⚠️ Columna 'Edad' no encontrada")
            return data
        
        def map_age_groups(age):
            if age <= 19:
                return 0  # 18-19
            elif age <= 24:
                return 1  # 20-24
            elif age <= 34:
                return 2  # 25-34
            elif age <= 49:
                return 3  # 35-49
            else:
                return 4  # ≥50
        
        data['rango_edad'] = data['Edad'].apply(map_age_groups).astype('int8')
        data['rango_edad'] = data['rango_edad'].replace(4, 3)  # 4 → 3 (consolidar mayores de 50)
        
        distribucion = data['rango_edad'].value_counts().sort_index()
        print(f"   ✓ Rango edad creado:")
        for rango, count in distribucion.items():
            print(f"      - Rango {rango}: {count} registros")
        
        # ⚠️ ELIMINAR COLUMNA EDAD
        data = data.drop(columns=['Edad'])
        print("   ✓ Columna 'Edad' eliminada (ya no es necesaria)")
        
        return data
    
    # ============================================================================
    # FASE 11: APLICAR COLUMNAS DEL MODELO
    # ============================================================================
    
    def _aplicar_columnas_modelo(self, data):
        """Aplicar orden y selección de columnas del modelo (desde columnas.csv)"""
        print("\n📋 Aplicando columnas del modelo...")
        
        if not self.columnas_modelo:
            print("   ⚠️ Columnas del modelo no cargadas, saltando...")
            return data
        
        print(f"   📝 Seleccionando y ordenando columnas...")
        
        # PASO 1: Columnas que ya existen en data
        cols_existentes = [col for col in self.columnas_modelo if col in data.columns]
        
        # PASO 2: Columnas que NO existen pero tienen "_" → crear con 0
        cols_a_crear = [
            col for col in self.columnas_modelo 
            if col not in data.columns and "_" in col
        ]
        
        if cols_a_crear:
            print(f"      ✓ Creando {len(cols_a_crear)} columnas faltantes con 0")
            for col in cols_a_crear:
                data[col] = 0
        
        # PASO 3: Construir data solo con las columnas deseadas
        columnas_finales = cols_existentes + cols_a_crear
        
        # PASO 4: Ordenar según columnas_modelo (mantener el orden exacto)
        data = data[[col for col in self.columnas_modelo if col in columnas_finales]]
        
        print(f"   ✓ Columnas finales: {len(data.columns)}")
        print(f"      - Existentes: {len(cols_existentes)}")
        print(f"      - Creadas: {len(cols_a_crear)}")
        
        # Verificar que el orden es correcto
        if list(data.columns) == [col for col in self.columnas_modelo if col in columnas_finales]:
            print("   ✓ Orden de columnas: CORRECTO")
        else:
            print("   ⚠️ Orden de columnas: puede no coincidir exactamente")
        
        return data
    
    # ============================================================================
    # FASE 12: ELIMINAR COLUMNAS DUPLICADAS
    # ============================================================================
    
    def _eliminar_duplicados(self, data):
        """Detectar y eliminar columnas duplicadas"""
        print("\n🔍 Detectando columnas duplicadas...")
        
        # Verificar duplicados en nombres de columnas
        columnas_originales = len(data.columns)
        duplicados = data.columns[data.columns.duplicated()].tolist()
        
        if duplicados:
            print(f"   ⚠️ Encontradas {len(duplicados)} columnas duplicadas:")
            for dup in duplicados[:10]:  # Mostrar max 10
                print(f"      - {dup}")
            
            # Eliminar duplicados (mantiene la primera ocurrencia)
            data = data.loc[:, ~data.columns.duplicated()]
            print(f"   ✓ Duplicados eliminados: {columnas_originales} → {len(data.columns)} columnas")
        else:
            print("   ✓ No hay columnas duplicadas")
        
        # Verificar valores únicos en nombres
        columnas_unicas = len(set(data.columns))
        if columnas_unicas != len(data.columns):
            print(f"   ⚠️ Advertencia: {len(data.columns)} columnas pero solo {columnas_unicas} nombres únicos")
        
        return data


# =============================================================================
# FUNCIÓN PRINCIPAL PARA STREAMLIT
# =============================================================================

def procesar_ajustes_completo(data_encoded_df, per_original_df=None, columnas_path=None):
    """
    Función para usar en Streamlit que procesa ajustes finales
    
    Args:
        data_encoded_df: DataFrame resultado del encoding
        per_original_df: DataFrame PER original (opcional, para validación)
        columnas_path: Ruta al CSV con columnas del modelo (columnas.csv)
        
    Returns:
        DataFrame listo para predicción
    """
    procesador = DataProcessorAjustes(per_original_df, columnas_path)
    data_final = procesador.procesar(data_encoded_df)
    return data_final


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    procesador = DataProcessorAjustes()
    print("\n✅ Procesador de ajustes listo")
