"""
Data Processor COMPLETO - Limpieza hasta antes de dumificación
Replica EXACTAMENTE el pipeline_streamlit.py hasta el punto de encoding
Incluye TODOS los pasos de limpieza, transformación, y cálculo de variables
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Tuple

class DataProcessorLimpiezaCompleto:
    """
    Procesador que replica TODOS los pasos del pipeline hasta antes de dumificación
    """
    
    def __init__(self):
        """Inicializa el procesador"""
        print("✅ Procesador de Limpieza COMPLETO inicializado")
    
    def procesar_desde_excel(self, archivo_path: str) -> pd.DataFrame:
        """
        Procesa un archivo Excel con 4 hojas y retorna DataFrame limpio
        
        Args:
            archivo_path: Ruta al archivo Excel
            
        Returns:
            DataFrame limpio listo para encoding
        """
        print(f"\n📂 Leyendo archivo: {archivo_path}")
        
        # Leer las 4 hojas
        notas = pd.read_excel(archivo_path, sheet_name='NOTAS')
        per = pd.read_excel(archivo_path, sheet_name='PER')
        prom = pd.read_excel(archivo_path, sheet_name='PROM')
        adm = pd.read_excel(archivo_path, sheet_name='ADM')
        
        print(f"   ✓ NOTAS: {len(notas)} registros")
        print(f"   ✓ PER: {len(per)} registros")
        print(f"   ✓ PROM: {len(prom)} registros")
        print(f"   ✓ ADM: {len(adm)} registros")
        
        # Procesar
        return self.procesar_dataframes(notas, per, prom, adm)
    
    def procesar_dataframes(self, notas: pd.DataFrame, per: pd.DataFrame, 
                           prom: pd.DataFrame, adm: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa los 4 DataFrames con TODOS los pasos de limpieza
        
        Args:
            notas, per, prom, adm: DataFrames de las 4 bases
            
        Returns:
            DataFrame limpio (sin dumificación)
        """
        print("\n" + "="*80)
        print("🔄 INICIANDO PROCESAMIENTO COMPLETO - TODOS LOS PASOS")
        print("="*80)
        
        # ========== FASE 0: PREPARACIÓN DE NOTAS ==========
        print("\n" + "="*80)
        print("FASE 0: PREPARACIÓN DE NOTAS")
        print("="*80)
        
        # Paso -1: Limpieza inicial de NOTAS
        notas = self._paso_limpieza_inicial_notas(notas)
        
        # Paso 0A: Consolidación (crear estructura con Dropout)
        notas_consolidada = self._paso_consolidacion_inicial(notas)
        
        # Paso 0B: Métricas de calificaciones
        notas_consolidada = self._paso_metricas_calificaciones(notas, notas_consolidada)
        
        # Paso 0C: Métricas adicionales
        notas_consolidada = self._paso_metricas_adicionales(notas, notas_consolidada)
        
        # ========== FASE 1: FILTROS INICIALES ==========
        print("\n" + "="*80)
        print("FASE 1: FILTROS INICIALES")
        print("="*80)
        
        # Eliminar ciclos máximos
        notas_consolidada, per, prom, adm = self._eliminar_ciclos_maximos(notas_consolidada, per, prom, adm)
        
        # Eliminar UCollege
        notas_consolidada, per, prom, adm = self._eliminar_ucollege(notas_consolidada, per, prom, adm)
        
        # Filtrar ADM activos
        adm = self._filtrar_adm_activos(adm)
        
        # IDs comunes
        notas_consolidada, per, prom, adm = self._filtrar_ids_comunes(notas_consolidada, per, prom, adm)
        
        # ========== FASE 2: RELLENAR CICLO ADMISIÓN ==========
        print("\n" + "="*80)
        print("FASE 2: RELLENAR CICLO ADMISIÓN")
        print("="*80)
        
        per, prom = self._rellenar_ciclo_admision(per, prom, adm)
        
        # Convertir Ciclo a numérico
        notas_consolidada, per, prom, adm = self._convertir_ciclo_numerico(notas_consolidada, per, prom, adm)
        
        # ========== FASE 3: ELIMINAR COLUMNAS Y RENOMBRAR ==========
        print("\n" + "="*80)
        print("FASE 3: ELIMINAR COLUMNAS Y RENOMBRAR")
        print("="*80)
        
        adm, per, prom = self._eliminar_columnas_innecesarias(adm, per, prom)
        adm, per, prom, notas_consolidada = self._renombrar_columnas(adm, per, prom, notas_consolidada)
        
        # ========== FASE 4: FILTROS DE CALIDAD ==========
        print("\n" + "="*80)
        print("FASE 4: FILTROS DE CALIDAD")
        print("="*80)
        
        # Eliminar fallecidos
        notas_consolidada, per, prom, adm = self._eliminar_fallecidos(notas_consolidada, per, prom, adm)
        
        # Filtrar ciclos 10/30
        notas_consolidada, per, prom, adm = self._filtrar_ciclos_10_30(notas_consolidada, per, prom, adm)
        
        # Filtrar créditos = 0
        per, prom = self._filtrar_creditos_cero(per, prom)
        
        # Transformar Mult Programa
        notas_consolidada, per, prom = self._transformar_mult_programa(notas_consolidada, per, prom)
        
        # ========== FASE 5: MERGE DE BASES ==========
        print("\n" + "="*80)
        print("FASE 5: MERGE DE BASES")
        print("="*80)
        
        data_fusionada = self._merge_todas_bases(per, prom, notas_consolidada, adm)
        
        # ========== FASE 6: RESOLVER DUPLICADOS ==========
        print("\n" + "="*80)
        print("FASE 6: RESOLVER DUPLICADOS")
        print("="*80)
        
        data_limpia = self._resolver_duplicados(data_fusionada)
        
        # ========== FASE 7: CALCULAR SIGLAS PROG ==========
        print("\n" + "="*80)
        print("FASE 7: CALCULAR SIGLAS PROG")
        print("="*80)
        
        data_con_siglas = self._calcular_siglas_prog(data_limpia)
        
        # ========== FASE 8: LIMPIEZA GEOGRÁFICA ==========
        print("\n" + "="*80)
        print("FASE 8: LIMPIEZA GEOGRÁFICA")
        print("="*80)
        
        data_geo = self._limpieza_geografica(data_con_siglas)
        
        # ========== FASE 9: RELLENAR DATOS FALTANTES ==========
        print("\n" + "="*80)
        print("FASE 9: RELLENAR DATOS FALTANTES")
        print("="*80)
        
        data_completa = self._rellenar_datos_faltantes(data_geo)
        
        # ========== FASE 10: CALCULAR EDAD ==========
        print("\n" + "="*80)
        print("FASE 10: CALCULAR EDAD")
        print("="*80)
        
        data_final = self._calcular_edad(data_completa)
        
        print("\n" + "="*80)
        print(f"✅ PROCESAMIENTO COMPLETADO")
        print(f"   • Registros finales: {len(data_final)}")
        print(f"   • Columnas finales: {len(data_final.columns)}")
        print(f"   • Listo para encoding")
        print("="*80)
        
        return data_final
    
    # ============================================================================
    # FASE 0: PREPARACIÓN DE NOTAS
    # ============================================================================
    
    def _paso_limpieza_inicial_notas(self, notas):
        """Limpieza inicial específica de NOTAS"""
        print("\n🧹 Limpieza inicial de NOTAS")
        
        # Renombrar Estado.1
        if 'Estado.1' in notas.columns:
            notas = notas.rename(columns={'Estado.1': 'Estado Clase'})
            print("   ✓ 'Estado.1' → 'Estado Clase'")
        
        # Eliminar columnas
        cols_drop = ['Nombre', 'Nº Oferta', 'Nº Clase', 'Sesión', 'Sección', 'Motivo']
        cols_found = [c for c in cols_drop if c in notas.columns]
        if cols_found:
            notas = notas.drop(columns=cols_found)
            print(f"   ✓ Eliminadas: {cols_found}")
        
        return notas
    
    def _paso_consolidacion_inicial(self, notas):
        """PASO 1: Consolidación (estructura base + Dropout)"""
        print("\n🏗️ PASO 1: Consolidación (estructura base + Dropout)")
        
        estados_desercion = ["Suspendido", "Permiso", "Interrumpido", "Expulsado", "Cancelado"]
        
        # Identificar columnas
        col_id = 'ID'
        col_ciclo = 'Ciclo'
        col_grado = next((c for c in ['Grado Académico', 'Grado_Academico'] if c in notas.columns), None)
        col_programa = next((c for c in ['Programa Académico Base', 'Programa_Academico_Base'] if c in notas.columns), None)
        col_estado = next((c for c in ['Estado', 'Estado Clase'] if c in notas.columns), None)
        
        # Crear agrupación
        columnas_agrupacion = [col_id, col_grado, col_ciclo]
        df_unico = notas[columnas_agrupacion + [col_programa, col_estado]].drop_duplicates()
        
        agrupacion_base = df_unico.groupby(columnas_agrupacion).agg({
            col_programa: lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0],
            col_estado: lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0]
        }).reset_index()
        
        # Renombrar
        rename_dict = {
            col_id: 'ID',
            col_grado: 'Grado_Academico',
            col_ciclo: 'Ciclo',
            col_programa: 'Programa_Academico_Base',
            col_estado: 'Estado'
        }
        agrupacion_base = agrupacion_base.rename(columns=rename_dict)
        
        # Dropout
        agrupacion_base['Dropout'] = agrupacion_base['Estado'].apply(
            lambda x: 1 if x in estados_desercion else 0
        )
        
        print(f"   ✓ Consolidados: {len(agrupacion_base)} registros")
        print(f"   ✓ Dropout=0: {(agrupacion_base['Dropout']==0).sum()}")
        print(f"   ✓ Dropout=1: {(agrupacion_base['Dropout']==1).sum()}")
        
        return agrupacion_base
    
    def _paso_metricas_calificaciones(self, notas_original, notas_consolidada):
        """PASO 2: Calcular métricas de calificaciones"""
        print("\n📊 PASO 2: Métricas de calificaciones")
        
        col_id = 'ID'
        col_ciclo = 'Ciclo'
        col_calif = 'Calif'
        col_creditos = 'Uni Matrd'
        col_grado = next((c for c in ['Grado Académico', 'Grado_Academico'] if c in notas_original.columns), None)
        col_id_curso = 'ID Curso' if 'ID Curso' in notas_original.columns else None
        col_descripcion = 'Descripción' if 'Descripción' in notas_original.columns else None
        
        # Filtrar válidos
        mask_validos = (
            notas_original[col_calif].notna() &
            notas_original[col_creditos].notna() &
            (notas_original[col_creditos] > 0)
        )
        df_validos = notas_original[mask_validos].copy()
        
        # Calcular métricas
        grupos = df_validos.groupby([col_id, col_grado, col_ciclo])
        metricas_lista = []
        
        for (id_est, grado, ciclo), grupo in grupos:
            califs = grupo[col_calif].values
            creditos = grupo[col_creditos].values
            
            if len(califs) == 0:
                continue
            
            promedio = np.average(califs, weights=creditos)
            desviacion = np.sqrt(np.average((califs - promedio)**2, weights=creditos)) if len(califs) > 1 else 0.0
            
            idx_min = grupo[col_calif].idxmin()
            idx_max = grupo[col_calif].idxmax()
            
            contribuciones = califs * creditos
            
            metricas_lista.append({
                'ID': id_est,
                'Grado_Academico': grado,
                'Ciclo': ciclo,
                'Promedio_Ciclo': round(promedio, 2),
                'Des_Estandar_Ciclo': round(desviacion, 2),
                'Min_Ciclo': round(grupo.loc[idx_min, col_calif], 2),
                'Cred_Min_Calif_Ciclo': grupo.loc[idx_min, col_creditos],
                'ID_Min_Ciclo': grupo.loc[idx_min, col_id_curso] if col_id_curso else '',
                'Clase_Min_Ciclo': str(grupo.loc[idx_min, col_descripcion]) if col_descripcion else 'Sin datos',
                'Max_Ciclo': round(grupo.loc[idx_max, col_calif], 2),
                'Cred_Max_Calif_Ciclo': grupo.loc[idx_max, col_creditos],
                'ID_Max_Ciclo': grupo.loc[idx_max, col_id_curso] if col_id_curso else '',
                'Clase_Max_Ciclo': str(grupo.loc[idx_max, col_descripcion]) if col_descripcion else 'Sin datos',
                'Rango_Ponderado_Ciclo': round(contribuciones.max() - contribuciones.min(), 2)
            })
        
        metricas_df = pd.DataFrame(metricas_lista)
        notas_con_metricas = notas_consolidada.merge(metricas_df, on=['ID', 'Grado_Academico', 'Ciclo'], how='left')
        notas_con_metricas['Clase_Min_Ciclo'] = notas_con_metricas['Clase_Min_Ciclo'].fillna('Sin datos')
        notas_con_metricas['Clase_Max_Ciclo'] = notas_con_metricas['Clase_Max_Ciclo'].fillna('Sin datos')
        
        print(f"   ✓ Métricas calculadas para {len(metricas_df)} grupos")
        
        return notas_con_metricas
    
    def _paso_metricas_adicionales(self, notas_original, notas_consolidada):
        """PASO 3: Métricas adicionales (Num_Materias, Cant_Perdidas, Materias_Vistas)"""
        print("\n📊 PASO 3: Métricas adicionales")
        
        col_id = 'ID'
        col_ciclo = 'Ciclo'
        col_calif = 'Calif'
        col_programa = next((c for c in ['Programa Académico Base', 'Programa_Academico_Base'] if c in notas_original.columns), None)
        col_estado = next((c for c in ['Estado', 'Estado Clase'] if c in notas_original.columns), None)
        
        grouped = notas_original.groupby([col_id, col_programa, col_ciclo]).agg(
            Num_Materias_Ciclo=(col_id, 'count'),
            Cant_Perdidas=(col_calif, lambda x: (x < 3).sum()),
            Materias_Vistas=(col_estado, lambda x: (x == 'E').sum())
        ).reset_index()
        
        grouped = grouped.rename(columns={col_programa: 'Programa_Academico_Base'})
        notas_final = notas_consolidada.merge(grouped, on=['ID', 'Programa_Academico_Base', 'Ciclo'], how='left')
        
        print("   ✓ Agregadas: Num_Materias_Ciclo, Cant_Perdidas, Materias_Vistas")
        
        return notas_final
    
    # ============================================================================
    # FASE 1: FILTROS INICIALES
    # ============================================================================
    
    def _eliminar_ciclos_maximos(self, notas, per, prom, adm):
        """Eliminar ciclos máximos de cada base"""
        print("\n🗑️ Eliminando ciclos máximos...")
        
        ciclo_max_per = per["Ciclo"].max()
        ciclo_max_prom = prom["Ciclo"].max()
        ciclo_max_adm = adm["Ciclo"].max()
        ciclo_max_notas = notas["Ciclo"].max()
        
        per = per[per["Ciclo"] != ciclo_max_per].copy()
        prom = prom[prom["Ciclo"] != ciclo_max_prom].copy()
        adm = adm[adm["Ciclo"] != ciclo_max_adm].copy()
        notas = notas[notas["Ciclo"] != ciclo_max_notas].copy()
        
        print(f"   ✓ Eliminado ciclo max: PER={ciclo_max_per}, PROM={ciclo_max_prom}, ADM={ciclo_max_adm}, NOTAS={ciclo_max_notas}")
        
        return notas, per, prom, adm
    
    def _eliminar_ucollege(self, notas, per, prom, adm):
        """Eliminar UCollege Javeriano"""
        print("\n🗑️ Eliminando UCollege Javeriano...")
        
        per = per[per["Programa"] != "UCollege Javeriano"].copy()
        prom = prom[prom["Programa"] != "UCollege Javeriano"].copy()
        adm = adm[adm["Programa Académico"] != "UCollege Javeriano"].copy()
        notas = notas[notas["Programa_Academico_Base"] != "UCOLL"].copy()
        
        print("   ✓ UCollege eliminado de todas las bases")
        
        return notas, per, prom, adm
    
    def _filtrar_adm_activos(self, adm):
        """Filtrar solo activos en ADM"""
        print("\n✅ Filtrando ADM: solo 'Activo en Programa'...")
        
        antes = len(adm)
        adm = adm[adm["Estado.1"] == "Activo en Programa"].copy()
        despues = len(adm)
        
        print(f"   ✓ ADM: {antes} → {despues} ({antes-despues} eliminados)")
        
        return adm
    
    def _filtrar_ids_comunes(self, notas, per, prom, adm):
        """Filtrar IDs comunes en las 4 bases"""
        print("\n🔗 Filtrando IDs comunes...")
        
        ids_comunes = set(notas["ID"]) & set(per["ID"]) & set(prom["ID"]) & set(adm["ID"])
        
        notas = notas[notas["ID"].isin(ids_comunes)].copy()
        per = per[per["ID"].isin(ids_comunes)].copy()
        prom = prom[prom["ID"].isin(ids_comunes)].copy()
        adm = adm[adm["ID"].isin(ids_comunes)].copy()
        
        print(f"   ✓ IDs comunes: {len(ids_comunes)}")
        
        return notas, per, prom, adm
    
    # ============================================================================
    # FASE 2: RELLENAR CICLO ADMISIÓN
    # ============================================================================
    
    def _rellenar_ciclo_admision(self, per, prom, adm):
        """Rellenar Ciclo Admisión en PER y PROM desde ADM"""
        print("\n📝 Rellenando Ciclo Admisión...")
        
        id_prog_to_ciclo = adm.set_index(["ID", "Programa Académico"])["Ciclo"].to_dict()
        
        def rellenar_per(row):
            if pd.isna(row.get("Ccl Admis")) or row.get("Ccl Admis") == "":
                return id_prog_to_ciclo.get((row["ID"], row["Programa"]), row.get("Ccl Admis"))
            return row.get("Ccl Admis")
        
        def rellenar_prom(row):
            if pd.isna(row.get("Ciclo Admisión")) or row.get("Ciclo Admisión") == "":
                return id_prog_to_ciclo.get((row["ID"], row["Programa"]), row.get("Ciclo Admisión"))
            return row.get("Ciclo Admisión")
        
        per = per.copy()
        prom = prom.copy()
        
        if "Ccl Admis" in per.columns:
            per["Ccl Admis"] = per.apply(rellenar_per, axis=1)
            print("   ✓ PER: Ciclo Admisión rellenado")
        
        if "Ciclo Admisión" in prom.columns:
            prom["Ciclo Admisión"] = prom.apply(rellenar_prom, axis=1)
            print("   ✓ PROM: Ciclo Admisión rellenado")
        
        return per, prom
    
    def _convertir_ciclo_numerico(self, notas, per, prom, adm):
        """Convertir Ciclo a numérico en todas las bases"""
        print("\n🔢 Convirtiendo Ciclo a numérico...")
        
        for df in [notas, per, prom, adm]:
            if "Ciclo" in df.columns:
                df["Ciclo"] = pd.to_numeric(df["Ciclo"], errors="coerce").astype("Int64")
        
        print("   ✓ Ciclo convertido a Int64 en todas las bases")
        
        return notas, per, prom, adm
    
    # ============================================================================
    # FASE 3: ELIMINAR COLUMNAS Y RENOMBRAR
    # ============================================================================
    
    def _eliminar_columnas_innecesarias(self, adm, per, prom):
        """Eliminar columnas innecesarias"""
        print("\n🗑️ Eliminando columnas innecesarias...")
        
        cols_adm = ['Nombre', 'Tipo Doc ID', 'Doc ID', 'Nº Solic', 'Prefijo', 'Teléfono', 
                    'Dirección 1', 'Dirección 2', 'Tipo', 'Correo-E', 'Otro Correo E', 
                    'Prog Acad.1', 'Celular Inscripción']
        cols_adm_found = [c for c in cols_adm if c in adm.columns]
        if cols_adm_found:
            adm = adm.drop(columns=cols_adm_found)
            print(f"   ✓ ADM: {len(cols_adm_found)} columnas eliminadas")
        
        cols_prom = ['Nombres', 'Apellidos', '2º Apellido', 'Tipo Doc Identidad', 'Doc Identidad', 'Año']
        cols_prom_found = [c for c in cols_prom if c in prom.columns]
        if cols_prom_found:
            prom = prom.drop(columns=cols_prom_found)
            print(f"   ✓ PROM: {len(cols_prom_found)} columnas eliminadas")
        
        cols_per = ['Tipo Doc ID', 'Doc ID', 'Nombre', '2º Nombre', 'Última', '2º Apellido', 
                    'Dirección', 'Teléfono', 'Correo-E', 'Año']
        cols_per_found = [c for c in cols_per if c in per.columns]
        if cols_per_found:
            per = per.drop(columns=cols_per_found)
            print(f"   ✓ PER: {len(cols_per_found)} columnas eliminadas")
        
        return adm, per, prom
    
    def _renombrar_columnas(self, adm, per, prom, notas):
        """Renombrar columnas en todas las bases"""
        print("\n📝 Renombrando columnas...")
        
        adm = adm.rename(columns={
            'Ciclo': 'Ciclo Admisión',
            'País': 'País Nacimiento',
            'Estado': 'Dpto Nacimiento',
            'Programa Académico': 'Programa',
            'Ciudad': 'Ciudad (Dirección)',
            'ID Org Ext': 'ID Colegio',
            'Descr': 'Colegio',
            'Estado.1': 'Estado'
        })
        print("   ✓ ADM renombrado")
        
        prom = prom.rename(columns={
            'Grado': 'Mult Programa',
            'Situacion Academica': 'Situacion Acad',
            'Créd.Inscrtos y Aprobdos Ciclo': 'Créd.Inscritos y Aprobados Ciclo',
            'Estado Programa Académico': 'Estado',
            'Acción Programa': 'Acción',
            'Motivo Accion': 'Motivo'
        })
        print("   ✓ PROM renombrado")
        
        per = per.rename(columns={
            'Grado Académico': 'Mult Programa',
            'Matrd Progr': 'Créditos Inscritos en Ciclo',
            'Cred. Aprob.': 'Créd.Inscritos y Aprobados Ciclo',
            'Ccl Admis': 'Ciclo Admisión',
            'Lugar Nacimiento': 'Ciudad Nacimiento',
            'Acc Prog': 'Acción',
            'Motivo Acción': 'Motivo'
        })
        print("   ✓ PER renombrado")
        
        notas = notas.rename(columns={
            'Grado_Academico': 'Mult Programa',
            'Programa_Academico_Base': 'Programa',
            'Promedio_Ciclo': 'Promedio Ciclo'
        })
        print("   ✓ NOTAS renombrado")
        
        return adm, per, prom, notas
    
    # ============================================================================
    # FASE 4: FILTROS DE CALIDAD
    # ============================================================================
    
    def _eliminar_fallecidos(self, notas, per, prom, adm):
        """Eliminar IDs fallecidos"""
        print("\n⚠️ Eliminando IDs fallecidos...")
        
        motivos_excluir = ["Fallecido", "Fallecido Grado Póstumo"]
        
        if 'Motivo' in per.columns:
            ids_fallecidos = set(per.loc[per["Motivo"].isin(motivos_excluir), "ID"])
            
            if len(ids_fallecidos) > 0:
                per = per[~per["ID"].isin(ids_fallecidos)].copy()
                prom = prom[~prom["ID"].isin(ids_fallecidos)].copy()
                notas = notas[~notas["ID"].isin(ids_fallecidos)].copy()
                adm = adm[~adm["ID"].isin(ids_fallecidos)].copy()
                print(f"   ✓ {len(ids_fallecidos)} IDs fallecidos eliminados")
        
        return notas, per, prom, adm
    
    def _filtrar_ciclos_10_30(self, notas, per, prom, adm):
        """Filtrar solo ciclos que terminan en 10 o 30"""
        print("\n🔍 Filtrando ciclos (solo 10/30)...")
        
        def filtrar(df, col):
            antes = len(df)
            df = df.copy()
            df[col] = df[col].astype(str).str.strip()
            df = df[df[col].str.endswith(("10", "30"))].copy()
            despues = len(df)
            return df, antes, despues
        
        adm, antes_adm, despues_adm = filtrar(adm, "Ciclo Admisión")
        notas, antes_notas, despues_notas = filtrar(notas, "Ciclo")
        prom, antes_prom, despues_prom = filtrar(prom, "Ciclo")
        per, antes_per, despues_per = filtrar(per, "Ciclo")
        
        print(f"   ✓ ADM: {antes_adm} → {despues_adm}")
        print(f"   ✓ NOTAS: {antes_notas} → {despues_notas}")
        print(f"   ✓ PROM: {antes_prom} → {despues_prom}")
        print(f"   ✓ PER: {antes_per} → {despues_per}")
        
        return notas, per, prom, adm
    
    def _filtrar_creditos_cero(self, per, prom):
        """Filtrar registros con 0 créditos"""
        print("\n🔍 Filtrando créditos = 0...")
        
        antes_per = len(per)
        antes_prom = len(prom)
        
        per = per[per["Créditos Inscritos en Ciclo"] != 0].copy()
        prom = prom[prom["Créditos Inscritos en Ciclo"] != 0].copy()
        
        print(f"   ✓ PER: {antes_per} → {len(per)}")
        print(f"   ✓ PROM: {antes_prom} → {len(prom)}")
        
        return per, prom
    
    def _transformar_mult_programa(self, notas, per, prom):
        """Transformar Mult Programa a códigos numéricos"""
        print("\n🔄 Transformando Mult Programa...")
        
        transformaciones = {
            'Pregrado': 1, 'PREG': 1, 'pregrado': 1, 'preg': 1,
            'Pregrado 2': 2, 'PRE2': 2, 'Segundo Pregrado': 2, 'pre2': 2, 'pregrado 2': 2,
            'Tercer Pregrado': 3, 'PRE3': 3, 'Pregrado 3': 3, 'pre3': 3, 'tercer pregrado': 3, 'pregrado 3': 3,
            'Cuarto Pregrado': 4, 'PRE4': 4, 'Pregrado 4': 4, 'pre4': 4, 'cuarto pregrado': 4, 'pregrado 4': 4
        }
        
        per = per.copy()
        prom = prom.copy()
        notas = notas.copy()
        
        per["Mult Programa"] = per["Mult Programa"].map(transformaciones).astype("Int64")
        prom["Mult Programa"] = prom["Mult Programa"].map(transformaciones).astype("Int64")
        notas["Mult Programa"] = notas["Mult Programa"].map(transformaciones).astype("Int64")
        
        print("   ✓ Mult Programa transformado a códigos numéricos")
        
        return notas, per, prom
    
    # ============================================================================
    # FASE 5: MERGE DE BASES
    # ============================================================================
    
    def _merge_todas_bases(self, per, prom, notas, adm):
        """Merge de todas las bases"""
        print("\n🔗 Merge de bases...")
        
        # 1. PER + PROM
        per_prom = per.merge(prom, on=['ID', 'Mult Programa', 'Programa', 'Ciclo'], 
                            how='inner', suffixes=('_per', '_prom'))
        print(f"   ✓ PER + PROM = {len(per_prom)} registros")
        
        # 2. (PER+PROM) + NOTAS (con match de primeras 2 letras)
        per_prom['Prog_Acad_2'] = per_prom['Prog Acad'].str[:2]
        notas['Programa_2'] = notas['Programa'].str[:2]
        
        per_prom_notas = pd.merge(per_prom, notas,
                                 left_on=['ID', 'Mult Programa', 'Ciclo', 'Prog_Acad_2'],
                                 right_on=['ID', 'Mult Programa', 'Ciclo', 'Programa_2'],
                                 how='inner')
        
        per_prom_notas = per_prom_notas.drop(columns=['Prog_Acad_2', 'Programa_2'])
        per_prom_notas = per_prom_notas.rename(columns={'Programa_x': 'Programa', 'Programa_y': 'Siglas Programa'})
        
        print(f"   ✓ (PER+PROM) + NOTAS = {len(per_prom_notas)} registros")
        
        # 3. (PER+PROM+NOTAS) + ADM
        data_completa = per_prom_notas.merge(adm, on=["ID", "Programa"], how="left", suffixes=("_ppn", "_adm"))
        
        print(f"   ✓ (PER+PROM+NOTAS) + ADM = {len(data_completa)} registros")
        
        return data_completa
    
    # ============================================================================
    # FASE 6: RESOLVER DUPLICADOS
    # ============================================================================
    
    def _resolver_duplicados(self, data):
        """Resolver columnas duplicadas y eliminar Acción/Motivo"""
        print("\n🧹 Resolviendo duplicados...")
        
        # Créd.Inscritos y Aprobados Ciclo (preferir _prom)
        if "Créd.Inscritos y Aprobados Ciclo_per" in data.columns:
            data = data.drop(columns=["Créd.Inscritos y Aprobados Ciclo_per"])
        if "Créd.Inscritos y Aprobados Ciclo_prom" in data.columns:
            data = data.rename(columns={"Créd.Inscritos y Aprobados Ciclo_prom": "Créd.Inscritos y Aprobados Ciclo"})
        
        # Ciudad (preferir _ppn)
        if "Ciudad (Dirección)_adm" in data.columns:
            data = data.drop(columns=["Ciudad (Dirección)_adm"])
        if "Ciudad (Dirección)_ppn" in data.columns:
            data = data.rename(columns={"Ciudad (Dirección)_ppn": "Ciudad (Dirección)"})
        
        # Ciclo Admisión (preferir _prom)
        if "Ciclo Admisión_per" in data.columns:
            data = data.drop(columns=["Ciclo Admisión_per"])
        
        # Sexo (preferir _ppn)
        if "Sexo_adm" in data.columns:
            data = data.drop(columns=["Sexo_adm"])
        if "Sexo_ppn" in data.columns:
            data = data.rename(columns={"Sexo_ppn": "Sexo"})
        
        # Eliminar Colegio
        cols_colegio = data.filter(regex="^Colegio").columns.tolist()
        if cols_colegio:
            data = data.drop(columns=cols_colegio)
        
        # F Nacimiento (preferir _ppn)
        if "F Nacimiento_adm" in data.columns:
            data = data.drop(columns=["F Nacimiento_adm"])
        if "F Nacimiento_ppn" in data.columns:
            data = data.rename(columns={"F Nacimiento_ppn": "F Nacimiento"})
        
        # Dpto Nacimiento (preferir _ppn)
        if "Dpto Nacimiento_adm" in data.columns:
            data = data.drop(columns=["Dpto Nacimiento_adm"])
        if "Dpto Nacimiento_ppn" in data.columns:
            data = data.rename(columns={"Dpto Nacimiento_ppn": "Dpto Nacimiento"})
        
        # País Nacimiento (preferir _ppn)
        if "País Nacimiento_adm" in data.columns:
            data = data.drop(columns=["País Nacimiento_adm"])
        if "País Nacimiento_ppn" in data.columns:
            data = data.rename(columns={"País Nacimiento_ppn": "País Nacimiento"})
        
        # Eliminar Siglas Programa
        if "Siglas Programa" in data.columns:
            data = data.drop(columns=["Siglas Programa"])
        
        # Eliminar Ciclo Admisión duplicado
        if "Ciclo Admisión_prom" in data.columns:
            data = data.drop(columns=["Ciclo Admisión_prom"])
        
        # Eliminar Dropout
        if "Dropout" in data.columns:
            data = data.drop(columns=["Dropout"])
        
        # Estado (preferir _per)
        if "Estado_prom" in data.columns and "Estado_ppn" in data.columns:
            data = data.drop(columns=["Estado_prom", "Estado_ppn"])
        if "Estado_per" in data.columns:
            data = data.rename(columns={"Estado_per": "Estado"})
        
        # ⚠️ ELIMINAR ACCIÓN Y MOTIVO
        accion_cols = data.filter(regex="^Acción").columns.tolist()
        if accion_cols:
            data = data.drop(columns=accion_cols)
            print(f"   ✓ Eliminadas {len(accion_cols)} columnas de Acción")
        
        motivo_cols = data.filter(regex="^Motivo").columns.tolist()
        if motivo_cols:
            data = data.drop(columns=motivo_cols)
            print(f"   ✓ Eliminadas {len(motivo_cols)} columnas de Motivo")
        
        print("   ✓ Duplicados resueltos")
        
        return data
    
    # ============================================================================
    # FASE 7: CALCULAR SIGLAS PROG
    # ============================================================================
    
    def _calcular_siglas_prog(self, data):
        """Calcular Siglas Prog usando moda"""
        print("\n📊 Calculando Siglas Prog...")
        
        # Moda de Prog Acad_ppn
        if "Prog Acad_ppn" in data.columns:
            moda_ppn = (
                data.groupby(["Mult Programa", "Programa"])["Prog Acad_ppn"]
                .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
                .reset_index()
                .rename(columns={"Prog Acad_ppn": "Prog Acad_ppn_moda"})
            )
            
            data = data.merge(moda_ppn, on=["Mult Programa", "Programa"], how="left")
            data["Prog Acad_ppn_normalizado"] = data["Prog Acad_ppn_moda"]
            data = data.drop(columns=["Prog Acad_ppn_moda"])
            print("   ✓ Prog Acad_ppn normalizado")
        
        # Moda de Prog Acad_adm (híbrido)
        if "Prog Acad_adm" in data.columns:
            moda_adm = (
                data.groupby(["Mult Programa", "Programa"])["Prog Acad_adm"]
                .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
                .reset_index()
                .rename(columns={"Prog Acad_adm": "Prog Acad_adm_moda"})
            )
            
            data = data.merge(moda_adm, on=["Mult Programa", "Programa"], how="left")
            
            def normalizar_adm(row):
                original = row.get("Prog Acad_adm")
                moda = row.get("Prog Acad_adm_moda")
                if pd.isna(moda):
                    return original
                match = re.search(r"\d+$", str(original))
                if match:
                    return str(moda) + match.group()
                return moda
            
            data["Prog Acad_adm_normalizado"] = data.apply(normalizar_adm, axis=1)
            data = data.drop(columns=["Prog Acad_adm_moda"])
            print("   ✓ Prog Acad_adm normalizado")
        
        # Quitar penúltimo
        def quitar_penultimo(valor):
            if pd.isna(valor):
                return valor
            valor = str(valor)
            if len(valor) >= 6:
                return valor[:-2] + valor[-1]
            return valor
        
        if "Prog Acad_adm_normalizado" in data.columns:
            data["Prog Acad_adm_normalizado"] = data["Prog Acad_adm_normalizado"].apply(quitar_penultimo)
        
        # Crear Siglas Prog
        if "Prog Acad_ppn" in data.columns:
            data = data.drop(columns=["Prog Acad_ppn"])
        if "Prog Acad_adm" in data.columns:
            data = data.drop(columns=["Prog Acad_adm"])
        
        if "Prog Acad_ppn_normalizado" in data.columns:
            data = data.rename(columns={"Prog Acad_ppn_normalizado": "Siglas Prog"})
        
        if "Prog Acad_adm_normalizado" in data.columns:
            data = data.rename(columns={"Prog Acad_adm_normalizado": "Siglas Prog ADM"})
        
        # Eliminar extras
        cols_drop = ["Fecha Grado", "Estado_adm", "Siglas Prog ADM"]
        cols_found = [c for c in cols_drop if c in data.columns]
        if cols_found:
            data = data.drop(columns=cols_found)
        
        print("   ✓ Siglas Prog creada")
        
        return data
    
    # ============================================================================
    # FASE 8: LIMPIEZA GEOGRÁFICA
    # ============================================================================
    
    def _limpieza_geografica(self, data):
        """Limpieza de ciudades y departamentos"""
        print("\n🗺️ Limpieza geográfica...")
        
        # Rellenar Dpto y País con "Otro"
        if "Dpto Nacimiento" in data.columns:
            nulos = data["Dpto Nacimiento"].isnull().sum()
            data["Dpto Nacimiento"] = data["Dpto Nacimiento"].fillna("Otro")
            print(f"   ✓ Dpto Nacimiento: {nulos} nulos → 'Otro'")
        
        if "País Nacimiento" in data.columns:
            nulos = data["País Nacimiento"].isnull().sum()
            data["País Nacimiento"] = data["País Nacimiento"].fillna("Otro")
            print(f"   ✓ País Nacimiento: {nulos} nulos → 'Otro'")
        
        # Crear internacional
        if "País Nacimiento" in data.columns:
            data["internacional"] = data["País Nacimiento"].apply(lambda x: 0 if x == "COL" else 1)
            print("   ✓ Variable 'internacional' creada")
        
        # Eliminar ID Colegio
        if "ID Colegio" in data.columns:
            data = data.drop(columns=["ID Colegio"])
        
        # Rellenar Ciudad desde Estado (Dirección)
        if "Ciudad (Dirección)" in data.columns and "Estado (Dirección)" in data.columns:
            mapa_ciudad_dpto = (
                data.dropna(subset=["Estado (Dirección)", "Ciudad (Dirección)"])
                .groupby("Estado (Dirección)")["Ciudad (Dirección)"]
                .agg(lambda x: x.mode().iloc[0])
                .to_dict()
            )
            
            data["Ciudad (Dirección)"] = data.apply(
                lambda row: mapa_ciudad_dpto.get(row["Estado (Dirección)"], row["Ciudad (Dirección)"])
                if pd.isnull(row["Ciudad (Dirección)"]) and pd.notnull(row["Estado (Dirección)"])
                else row["Ciudad (Dirección)"],
                axis=1
            )
            print("   ✓ Ciudad rellenada desde Estado")
        
        # Reemplazar ciudades numéricas
        if "Ciudad (Dirección)" in data.columns:
            mask_numericos = data["Ciudad (Dirección)"].apply(lambda x: str(x).isdigit())
            if "Estado (Dirección)" in data.columns:
                mask_bog = mask_numericos & (data["Estado (Dirección)"] == "BOG")
                data.loc[mask_bog, "Ciudad (Dirección)"] = "BOG"
            
            mapeo_ciudades = {
                "25899": "Zipaquira", "25473": "Mosquera", "25214": "Cota",
                "25126": "Cajica", "25269": "El Rosal", "25175": "Chia",
                "25843": "Tocancipa", "5001": "Medellin"
            }
            
            data["Ciudad (Dirección)"] = data["Ciudad (Dirección)"].apply(
                lambda x: mapeo_ciudades.get(str(x).strip(), x)
            )
            
            # Rellenar nulos con "Otro"
            nulos = data["Ciudad (Dirección)"].isnull().sum()
            data["Ciudad (Dirección)"] = data["Ciudad (Dirección)"].fillna("Otro")
            print(f"   ✓ Ciudad: códigos reemplazados, {nulos} nulos → 'Otro'")
        
        # Eliminar columnas geográficas auxiliares
        cols_drop = ["Estado (Dirección)", "País (Dirección)", "Ciudad Nacimiento"]
        cols_found = [c for c in cols_drop if c in data.columns]
        if cols_found:
            data = data.drop(columns=cols_found)
        
        return data
    
    # ============================================================================
    # FASE 9: RELLENAR DATOS FALTANTES
    # ============================================================================
    
    def _rellenar_datos_faltantes(self, data):
        """Rellenar datos faltantes"""
        print("\n📝 Rellenando datos faltantes...")
        
        # Benef. Beca
        if "Benef. Beca" in data.columns:
            mask_vacios = data["Benef. Beca"].isnull() | (data["Benef. Beca"].astype(str).str.strip() == "")
            nulos = mask_vacios.sum()
            data.loc[mask_vacios, "Benef. Beca"] = "N"
            print(f"   ✓ Benef. Beca: {nulos} vacíos → 'N'")
        
        # Tipo Admisión
        if "Tipo Admisión" in data.columns:
            moda = data["Tipo Admisión"].mode().iloc[0] if len(data["Tipo Admisión"].mode()) > 0 else "TRL"
            mask_vacios = data["Tipo Admisión"].isnull() | (data["Tipo Admisión"].astype(str).str.strip() == "")
            nulos = mask_vacios.sum()
            data.loc[mask_vacios, "Tipo Admisión"] = moda
            print(f"   ✓ Tipo Admisión: {nulos} vacíos → '{moda}'")
        
        return data
    
    # ============================================================================
    # FASE 10: CALCULAR EDAD
    # ============================================================================
    
    def _calcular_edad(self, data):
        """Calcular edad desde F Nacimiento o moda por ciclo"""
        print("\n🎂 Calculando edad...")
        
        if "F Nacimiento" not in data.columns or "Ciclo" not in data.columns:
            print("   ⚠️ No se puede calcular edad (faltan columnas)")
            return data
        
        # Convertir Ciclo a fecha
        def ciclo_a_fecha(ciclo):
            try:
                ciclo_int = int(ciclo)
                ciclo_str = str(ciclo_int).zfill(4)
                anio_num = int(ciclo_str[:-2])
                anio = 2000 + anio_num
                ultimos_dos = int(ciclo_str[-2:])
                mes = 1 if ultimos_dos == 10 else 7
                return datetime(anio, mes, 20)
            except:
                return None
        
        data["Fecha_Ciclo"] = data["Ciclo"].apply(ciclo_a_fecha)
        
        # Calcular edad
        def calcular_edad_anos(nacimiento, fecha_ciclo):
            if pd.isnull(nacimiento) or pd.isnull(fecha_ciclo):
                return pd.NA
            try:
                edad = fecha_ciclo.year - nacimiento.year
                if (fecha_ciclo.month, fecha_ciclo.day) < (nacimiento.month, nacimiento.day):
                    edad -= 1
                return edad
            except:
                return pd.NA
        
        data["Edad"] = data.apply(lambda row: calcular_edad_anos(row["F Nacimiento"], row["Fecha_Ciclo"]), axis=1)
        
        # Rellenar edad nula con moda por ciclo
        def ciclo_a_anio(ciclo):
            try:
                ciclo = int(ciclo)
                return 2000 + ciclo // 100
            except:
                return None
        
        data["Anio_Ciclo"] = data["Ciclo"].apply(ciclo_a_anio)
        moda_por_ciclo = data.groupby("Ciclo")["Edad"].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        
        rellenados = 0
        for student_id, group in data[data["Edad"].isna()].groupby("ID"):
            group_sorted = group.sort_values("Ciclo")
            primer_ciclo = group_sorted.iloc[0]["Ciclo"]
            anio_inicial = group_sorted.iloc[0]["Anio_Ciclo"]
            edad_inicial = moda_por_ciclo.get(primer_ciclo, None)
            
            if edad_inicial is not None:
                edad_actual = edad_inicial
                anio_anterior = anio_inicial
                
                for idx, row in group_sorted.iterrows():
                    anio_ciclo = row["Anio_Ciclo"]
                    if anio_ciclo > anio_anterior:
                        edad_actual += anio_ciclo - anio_anterior
                        anio_anterior = anio_ciclo
                    data.at[idx, "Edad"] = edad_actual
                    rellenados += 1
        
        # Eliminar columnas auxiliares
        cols_drop = ["F Nacimiento", "Fecha_Ciclo", "Anio_Ciclo"]
        cols_found = [c for c in cols_drop if c in data.columns]
        if cols_found:
            data = data.drop(columns=cols_found)
        
        print(f"   ✓ Edad calculada ({rellenados} valores rellenados con moda)")
        print(f"   ✓ Nulos restantes en Edad: {data['Edad'].isna().sum()}")
        
        return data


# =============================================================================
# FUNCIÓN PRINCIPAL PARA STREAMLIT
# =============================================================================

def procesar_limpieza_completa(notas_df, per_df, prom_df, adm_df):
    """
    Función para usar en Streamlit que procesa y retorna DataFrame limpio
    
    Args:
        notas_df, per_df, prom_df, adm_df: DataFrames de las 4 hojas
        
    Returns:
        DataFrame limpio (sin dumificación)
    """
    procesador = DataProcessorLimpiezaCompleto()
    data_limpia = procesador.procesar_dataframes(notas_df, per_df, prom_df, adm_df)
    return data_limpia


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    procesador = DataProcessorLimpiezaCompleto()
    # data_limpia = procesador.procesar_desde_excel("tu_archivo.xlsx")
    print("\n✅ Procesador de limpieza COMPLETO listo")
