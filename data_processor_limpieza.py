"""
Data Processor - PARTE 1: LIMPIEZA Y PREPARACIÓN
Replica exactamente los pasos 1-13 del pipeline_streamlit.py
"""

import pandas as pd
import numpy as np
import re
from typing import Tuple

class DataProcessorLimpieza:
    """
    Procesador que replica los pasos 1-13 del pipeline streamlit
    Genera base de datos limpia y preparada para encoding
    """
    
    def __init__(self):
        """Inicializa el procesador"""
        print("✅ Procesador de Limpieza inicializado")
    
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
        Procesa los 4 DataFrames y retorna base limpia
        
        Args:
            notas, per, prom, adm: DataFrames de las 4 bases
            
        Returns:
            DataFrame limpio
        """
        print("\n" + "="*80)
        print("🔄 INICIANDO LIMPIEZA - 13 PASOS")
        print("="*80)
        
        # PASO 1-4: Renombres
        notas, per, prom, adm = self._paso_1_4_renombres(notas, per, prom, adm)
        
        # PASO 5: Eliminar IDs fallecidos
        notas, per, prom, adm = self._paso_5_eliminar_fallecidos(notas, per, prom, adm)
        
        # PASO 6: Filtrar ciclos y créditos
        notas, per, prom, adm = self._paso_6_filtrar_ciclos(notas, per, prom, adm)
        
        # PASO 7: Transformar Mult Programa
        notas, per, prom = self._paso_7_transformar_mult_programa(notas, per, prom)
        
        # PASO 8: Merge PER + PROM + NOTAS
        per_prom_notas = self._paso_8_merge_per_prom_notas(per, prom, notas)
        
        # PASO 9: Merge con ADM
        data_fusionada = self._paso_9_merge_adm(per_prom_notas, adm)
        
        # PASO 10: Resolver duplicados y eliminar Acción/Motivo
        data_limpia = self._paso_10_resolver_duplicados(data_fusionada)
        
        # PASO 11: Calcular Siglas Prog (moda)
        data_con_siglas = self._paso_11_calcular_siglas_prog(data_limpia)
        
        # PASO 12: Quitar penúltimo y crear Siglas Prog
        data_final = self._paso_12_crear_siglas_prog(data_con_siglas)
        
        # PASO 13: Rellenar Dpto y País Nacimiento
        data_final = self._paso_13_rellenar_dpto_pais(data_final)
        
        print("\n" + "="*80)
        print(f"✅ LIMPIEZA COMPLETADA")
        print(f"   • Registros finales: {len(data_final)}")
        print(f"   • Columnas finales: {len(data_final.columns)}")
        print("="*80)
        
        return data_final
    
    def _paso_1_4_renombres(self, notas, per, prom, adm):
        """PASO 1-4: Renombrar columnas en las 4 bases"""
        print("\n📝 PASO 1-4: Renombrando columnas...")
        
        # PASO 1: ADM
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
        
        # PASO 2: PROM
        prom = prom.rename(columns={
            'Grado': 'Mult Programa',
            'Situacion Academica': 'Situacion Acad',
            'Créd.Inscrtos y Aprobdos Ciclo': 'Créd.Inscritos y Aprobados Ciclo',
            'Estado Programa Académico': 'Estado',
            'Acción Programa': 'Acción',
            'Motivo Accion': 'Motivo'
        })
        
        # PASO 3: PER
        per = per.rename(columns={
            'Grado Académico': 'Mult Programa',
            'Matrd Progr': 'Créditos Inscritos en Ciclo',
            'Cred. Aprob.': 'Créd.Inscritos y Aprobados Ciclo',
            'Ccl Admis': 'Ciclo Admisión',
            'Lugar Nacimiento': 'Ciudad Nacimiento',
            'Acc Prog': 'Acción',
            'Motivo Acción': 'Motivo'
        })
        
        # PASO 4: NOTAS
        notas = notas.rename(columns={
            'Grado_Academico': 'Mult Programa',
            'Programa_Academico_Base': 'Programa'
        })
        
        print("   ✓ Renombres completados")
        return notas, per, prom, adm
    
    def _paso_5_eliminar_fallecidos(self, notas, per, prom, adm):
        """PASO 5: Eliminar IDs fallecidos"""
        print("\n⚠️  PASO 5: Eliminando IDs fallecidos...")
        
        # Motivos a excluir
        motivos_excluir = ["Fallecido", "Fallecido Grado Póstumo"]
        
        # Identificar IDs con esos motivos en PER
        if 'Motivo' in per.columns:
            ids_fallecidos = set(per.loc[per["Motivo"].isin(motivos_excluir), "ID"])
            
            if len(ids_fallecidos) > 0:
                print(f"   → {len(ids_fallecidos)} IDs fallecidos identificados")
                
                # Eliminar de todas las bases
                per = per[~per["ID"].isin(ids_fallecidos)].copy()
                prom = prom[~prom["ID"].isin(ids_fallecidos)].copy()
                notas = notas[~notas["ID"].isin(ids_fallecidos)].copy()
                adm = adm[~adm["ID"].isin(ids_fallecidos)].copy()
                
                print(f"   ✓ IDs eliminados de todas las bases")
            else:
                print("   ✓ No se encontraron IDs fallecidos")
        else:
            print("   ⚠️  Columna 'Motivo' no encontrada en PER")
        
        return notas, per, prom, adm
    
    def _paso_6_filtrar_ciclos(self, notas, per, prom, adm):
        """PASO 6: Filtrar ciclos y créditos"""
        print("\n🔍 PASO 6: Filtrando ciclos y créditos...")
        
        def filtrar_ciclo(df, col):
            """Función auxiliar para filtrar ciclos"""
            if col not in df.columns:
                return df
            
            antes = len(df)
            df = df.copy()
            df[col] = df[col].astype(str).str.strip()
            df = df[df[col].str.endswith(('10', '30'))].copy()
            despues = len(df)
            
            print(f"   → {col}: {antes} → {despues} registros ({antes-despues} eliminados)")
            return df
        
        # Aplicar filtro a cada base
        adm = filtrar_ciclo(adm, "Ciclo Admisión")
        notas = filtrar_ciclo(notas, "Ciclo")
        prom = filtrar_ciclo(prom, "Ciclo")
        per = filtrar_ciclo(per, "Ciclo")
        
        # Eliminar registros con 0 créditos
        if 'Créditos Inscritos en Ciclo' in per.columns:
            antes_per = len(per)
            per = per[per["Créditos Inscritos en Ciclo"] != 0].copy()
            print(f"   → PER (0 créditos): {antes_per} → {len(per)} registros")
        
        if 'Créditos Inscritos en Ciclo' in prom.columns:
            antes_prom = len(prom)
            prom = prom[prom["Créditos Inscritos en Ciclo"] != 0].copy()
            print(f"   → PROM (0 créditos): {antes_prom} → {len(prom)} registros")
        
        print("   ✓ Filtros aplicados")
        return notas, per, prom, adm
    
    def _paso_7_transformar_mult_programa(self, notas, per, prom):
        """PASO 7: Transformar Mult Programa a códigos numéricos"""
        print("\n🔄 PASO 7: Transformando Mult Programa...")
        
        transformaciones = {
            'Pregrado': 1,
            'Especialización': 2,
            'Maestría': 3,
            'Doctorado': 4,
            'Especialidad Médica': 5,
            'Especialidad Odontológica': 6
        }
        
        def transformar(df):
            if 'Mult Programa' not in df.columns:
                return df
            
            df = df.copy()
            df["Mult Programa"] = df["Mult Programa"].map(transformaciones).astype("Int64")
            return df
        
        per = transformar(per)
        prom = transformar(prom)
        notas = transformar(notas)
        
        print("   ✓ Mult Programa transformado a códigos numéricos")
        return notas, per, prom
    
    def _paso_8_merge_per_prom_notas(self, per, prom, notas):
        """PASO 8: Merge PER + PROM + NOTAS"""
        print("\n🔗 PASO 8: Merge PER + PROM + NOTAS...")
        
        # 1. Merge PER + PROM
        per_prom_unido = per.merge(
            prom,
            on=['ID', 'Mult Programa', 'Programa', 'Ciclo'],
            how='inner',
            suffixes=('_per', '_prom')
        )
        print(f"   → PER + PROM = {len(per_prom_unido)} registros")
        
        # 2. Crear columnas auxiliares para match (primeras 2 letras)
        per_prom_unido['Prog_Acad_2'] = per_prom_unido['Prog Acad'].str[:2]
        notas['Programa_2'] = notas['Programa'].str[:2]
        
        # 3. Merge (PER+PROM) + NOTAS
        per_prom_notas = pd.merge(
            per_prom_unido,
            notas,
            left_on=['ID', 'Mult Programa', 'Ciclo', 'Prog_Acad_2'],
            right_on=['ID', 'Mult Programa', 'Ciclo', 'Programa_2'],
            how='inner'
        )
        print(f"   → (PER+PROM) + NOTAS = {len(per_prom_notas)} registros")
        
        # 4. Eliminar columnas auxiliares
        per_prom_notas = per_prom_notas.drop(columns=['Prog_Acad_2', 'Programa_2'])
        
        # 5. Renombrar Programa de NOTAS
        per_prom_notas = per_prom_notas.rename(columns={
            'Programa_y': 'Programa Notas'
        })
        
        print("   ✓ Merge PER+PROM+NOTAS completado")
        return per_prom_notas
    
    def _paso_9_merge_adm(self, per_prom_notas, adm):
        """PASO 9: Merge con ADM"""
        print("\n🔗 PASO 9: Merge con ADM...")
        
        # 1. Merge con ADM
        per_prom_notas_adm = per_prom_notas.merge(
            adm,
            on=["ID", "Programa"],
            how="left",
            suffixes=("_ppn", "_adm")
        )
        print(f"   → (PER+PROM+NOTAS) + ADM = {len(per_prom_notas_adm)} registros")
        
        # 2. Resolver duplicados (preferir _ppn sobre _adm, _prom sobre _per)
        
        # Créd.Inscritos y Aprobados Ciclo (preferir _prom)
        if "Créd.Inscritos y Aprobados Ciclo_per" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.drop(columns=["Créd.Inscritos y Aprobados Ciclo_per"])
        if "Créd.Inscritos y Aprobados Ciclo_prom" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.rename(columns={
                "Créd.Inscritos y Aprobados Ciclo_prom": "Créd.Inscritos y Aprobados Ciclo"
            })
        
        # Ciudad (Dirección) (preferir _ppn)
        if "Ciudad (Dirección)_adm" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.drop(columns=["Ciudad (Dirección)_adm"])
        if "Ciudad (Dirección)_ppn" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.rename(columns={
                "Ciudad (Dirección)_ppn": "Ciudad (Dirección)"
            })
        
        # Ciclo Admisión (preferir _prom)
        if "Ciclo Admisión_per" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.drop(columns=["Ciclo Admisión_per"])
        
        # Sexo (preferir _ppn)
        if "Sexo_adm" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.drop(columns=["Sexo_adm"])
        if "Sexo_ppn" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.rename(columns={
                "Sexo_ppn": "Sexo"
            })
        
        # Eliminar TODAS las columnas de Colegio
        cols_colegio = per_prom_notas_adm.filter(regex="^Colegio").columns.tolist()
        if cols_colegio:
            per_prom_notas_adm = per_prom_notas_adm.drop(columns=cols_colegio)
        
        # F Nacimiento (preferir _ppn)
        if "F Nacimiento_adm" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.drop(columns=["F Nacimiento_adm"])
        if "F Nacimiento_ppn" in per_prom_notas_adm.columns:
            per_prom_notas_adm = per_prom_notas_adm.rename(columns={
                "F Nacimiento_ppn": "F Nacimiento"
            })
        
        print("   ✓ Merge con ADM completado")
        return per_prom_notas_adm
    
    def _paso_10_resolver_duplicados(self, data):
        """PASO 10: Resolver duplicados y ELIMINAR Acción/Motivo"""
        print("\n🧹 PASO 10: Resolviendo duplicados y eliminando Acción/Motivo...")
        
        # Dpto Nacimiento (preferir _ppn)
        if "Dpto Nacimiento_adm" in data.columns:
            data = data.drop(columns=["Dpto Nacimiento_adm"])
        if "Dpto Nacimiento_ppn" in data.columns:
            data = data.rename(columns={
                "Dpto Nacimiento_ppn": "Dpto Nacimiento"
            })
        
        # País Nacimiento (preferir _ppn)
        if "País Nacimiento_adm" in data.columns:
            data = data.drop(columns=["País Nacimiento_adm"])
        if "País Nacimiento_ppn" in data.columns:
            data = data.rename(columns={
                "País Nacimiento_ppn": "País Nacimiento"
            })
        
        # Eliminar Siglas Programa si existe
        if "Siglas Programa" in data.columns:
            data = data.drop(columns=["Siglas Programa"])
        
        # Copiar a data_final
        data_final = data.copy()
        
        # Eliminar otros duplicados
        if "Ciclo Admisión_prom" in data_final.columns:
            data_final = data_final.drop(columns=["Ciclo Admisión_prom"])
        
        if "Dropout" in data_final.columns:
            data_final = data_final.drop(columns=["Dropout"])
        
        cols_estado = [c for c in ["Estado_prom", "Estado_ppn"] if c in data_final.columns]
        if cols_estado:
            data_final = data_final.drop(columns=cols_estado)
        
        if "Estado_per" in data_final.columns:
            data_final = data_final.rename(columns={"Estado_per": "Estado"})
        
        # ⚠️ CRÍTICO: ELIMINAR TODAS LAS COLUMNAS DE ACCIÓN
        accion_cols = data_final.filter(regex="^Acción").columns.tolist()
        if accion_cols:
            print(f"   → Eliminando {len(accion_cols)} columnas de Acción: {accion_cols}")
            data_final = data_final.drop(columns=accion_cols)
        
        # ⚠️ CRÍTICO: ELIMINAR TODAS LAS COLUMNAS DE MOTIVO
        motivo_cols = data_final.filter(regex="^Motivo").columns.tolist()
        if motivo_cols:
            print(f"   → Eliminando {len(motivo_cols)} columnas de Motivo: {motivo_cols}")
            data_final = data_final.drop(columns=motivo_cols)
        
        print("   ✓ Duplicados resueltos y Acción/Motivo eliminados")
        return data_final
    
    def _paso_11_calcular_siglas_prog(self, data):
        """PASO 11: Calcular Siglas Prog usando MODA"""
        print("\n📊 PASO 11: Calculando Siglas Prog (moda)...")
        
        # PARTE A: Calcular moda de Prog Acad_ppn
        if "Prog Acad_ppn" in data.columns:
            moda_por_grupo = (
                data.groupby(["Mult Programa", "Programa"])["Prog Acad_ppn"]
                .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
                .reset_index()
                .rename(columns={"Prog Acad_ppn": "Prog Acad_ppn_moda"})
            )
            
            data = data.merge(moda_por_grupo, on=["Mult Programa", "Programa"], how="left")
            data["Prog Acad_ppn_normalizado"] = data["Prog Acad_ppn_moda"]
            data = data.drop(columns=["Prog Acad_ppn_moda"])
            
            print(f"   ✓ Prog Acad_ppn normalizado calculado")
        
        # PARTE B: Calcular moda de Prog Acad_adm
        if "Prog Acad_adm" in data.columns:
            moda_por_grupo_adm = (
                data.groupby(["Mult Programa", "Programa"])["Prog Acad_adm"]
                .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
                .reset_index()
                .rename(columns={"Prog Acad_adm": "Prog Acad_adm_moda"})
            )
            
            data = data.merge(moda_por_grupo_adm, on=["Mult Programa", "Programa"], how="left")
            
            # Función de normalización híbrida
            def normalizar_adm(row):
                original = row.get("Prog Acad_adm")
                moda = row.get("Prog Acad_adm_moda")
                
                if pd.isna(moda):
                    return original
                
                # Buscar si el original tiene un número al final
                match = re.search(r"\d+$", str(original))
                if match:
                    numero = match.group()
                    return str(moda) + str(numero)
                else:
                    return moda
            
            data["Prog Acad_adm_normalizado"] = data.apply(normalizar_adm, axis=1)
            data = data.drop(columns=["Prog Acad_adm_moda"])
            
            print(f"   ✓ Prog Acad_adm normalizado calculado")
        
        print("   ✓ Siglas Prog (moda) calculadas")
        return data
    
    def _paso_12_crear_siglas_prog(self, data):
        """PASO 12: Quitar penúltimo carácter y crear Siglas Prog"""
        print("\n✂️  PASO 12: Creando Siglas Prog final...")
        
        # Función para quitar penúltimo carácter
        def quitar_penultimo(valor):
            if pd.isna(valor):
                return valor
            valor = str(valor)
            if len(valor) >= 6:
                return valor[:-2] + valor[-1]
            return valor
        
        # Aplicar a Prog Acad_adm_normalizado
        if "Prog Acad_adm_normalizado" in data.columns:
            data["Prog Acad_adm_normalizado"] = data["Prog Acad_adm_normalizado"].apply(quitar_penultimo)
        
        # Eliminar columnas originales
        cols_drop = ["Prog Acad_ppn", "Prog Acad_adm"]
        cols_drop = [c for c in cols_drop if c in data.columns]
        if cols_drop:
            data = data.drop(columns=cols_drop)
        
        # Renombrar normalizados a "Siglas Prog"
        if "Prog Acad_ppn_normalizado" in data.columns:
            data = data.rename(columns={
                "Prog Acad_ppn_normalizado": "Siglas Prog"
            })
        
        if "Prog Acad_adm_normalizado" in data.columns:
            data = data.rename(columns={
                "Prog Acad_adm_normalizado": "Siglas Prog ADM"
            })
        
        # Eliminar columnas adicionales
        cols_drop = ["Fecha Grado", "Estado_adm", "Siglas Prog ADM"]
        cols_drop = [c for c in cols_drop if c in data.columns]
        if cols_drop:
            data = data.drop(columns=cols_drop)
        
        print("   ✓ Siglas Prog creadas")
        return data
    
    def _paso_13_rellenar_dpto_pais(self, data):
        """PASO 13: Rellenar Dpto y País Nacimiento"""
        print("\n📝 PASO 13: Rellenando Dpto y País Nacimiento...")
        
        # Rellenar Dpto Nacimiento nulos con "Otro"
        if "Dpto Nacimiento" in data.columns:
            nulos_dpto = data["Dpto Nacimiento"].isnull().sum()
            if nulos_dpto > 0:
                data["Dpto Nacimiento"] = data["Dpto Nacimiento"].fillna("Otro")
                print(f"   → Dpto Nacimiento: {nulos_dpto} nulos rellenados con 'Otro'")
        
        # Rellenar País Nacimiento nulos con "Otro"
        if "País Nacimiento" in data.columns:
            nulos_pais = data["País Nacimiento"].isnull().sum()
            if nulos_pais > 0:
                data["País Nacimiento"] = data["País Nacimiento"].fillna("Otro")
                print(f"   → País Nacimiento: {nulos_pais} nulos rellenados con 'Otro'")
        
        # Crear columna "internacional"
        if "País Nacimiento" in data.columns:
            data["internacional"] = data["País Nacimiento"].apply(
                lambda x: 0 if x == "COL" else 1
            )
            print("   ✓ Columna 'internacional' creada")
        
        # Eliminar ID Colegio
        if "ID Colegio" in data.columns:
            data = data.drop(columns=["ID Colegio"])
        
        print("   ✓ Dpto y País Nacimiento rellenados")
        return data


# =============================================================================
# FUNCIÓN PRINCIPAL PARA STREAMLIT
# =============================================================================

def procesar_y_descargar_limpieza(notas_df, per_df, prom_df, adm_df):
    """
    Función para usar en Streamlit que procesa y retorna DataFrame limpio
    
    Args:
        notas_df, per_df, prom_df, adm_df: DataFrames de las 4 hojas
        
    Returns:
        DataFrame limpio listo para descargar
    """
    procesador = DataProcessorLimpieza()
    data_limpia = procesador.procesar_dataframes(notas_df, per_df, prom_df, adm_df)
    return data_limpia


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    # Ejemplo de cómo usar el procesador
    procesador = DataProcessorLimpieza()
    
    # Opción 1: Desde archivo Excel
    # data_limpia = procesador.procesar_desde_excel("tu_archivo.xlsx")
    
    # Opción 2: Desde DataFrames
    # data_limpia = procesador.procesar_dataframes(notas_df, per_df, prom_df, adm_df)
    
    # Guardar resultado
    # data_limpia.to_excel("base_limpia.xlsx", index=False)
    # data_limpia.to_csv("base_limpia.csv", index=False)
    
    print("\n✅ Procesador de limpieza listo para usar")
