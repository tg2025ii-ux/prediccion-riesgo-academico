"""
Data Processor ENCODING - Dumificación y preparación para predicción
Replica EXACTAMENTE el proceso de encoding del pipeline
Recibe base limpia y retorna base codificada lista para el modelo
"""

import pandas as pd
import numpy as np
import re
import unicodedata
from sklearn.preprocessing import OrdinalEncoder
from typing import Optional

class DataProcessorEncoding:
    """
    Procesador que transforma la base limpia en base codificada (con dummies)
    Lista para predicción
    """
    
    def __init__(self, mapa_categorias_path: Optional[str] = None):
        """
        Inicializa el procesador de encoding
        
        Args:
            mapa_categorias_path: Ruta al archivo Excel con categorías de materias
                                 (Libro1.xlsx con columnas 'Clase' y 'Categoría ')
        """
        self.mapa_categorias = None
        if mapa_categorias_path:
            self._cargar_mapa_categorias(mapa_categorias_path)
        print("✅ Procesador de Encoding inicializado")
    
    def _cargar_mapa_categorias(self, path: str):
        """Carga el mapa de categorías desde Excel"""
        try:
            categorias = pd.read_excel(path, sheet_name='Hoja1')
            self.mapa_categorias = dict(zip(categorias['Clase'], categorias['Categoría ']))
            print(f"   ✓ Mapa de categorías cargado: {len(self.mapa_categorias)} materias")
        except Exception as e:
            print(f"   ⚠️ Error al cargar mapa de categorías: {e}")
            self.mapa_categorias = {}
    
    def procesar(self, data_limpia: pd.DataFrame, 
                 mapa_categorias_path: Optional[str] = None) -> pd.DataFrame:
        """
        Procesa la base limpia y retorna base codificada
        
        Args:
            data_limpia: DataFrame resultado del procesador de limpieza
            mapa_categorias_path: Ruta al Excel con categorías (opcional si ya se pasó en __init__)
            
        Returns:
            DataFrame codificado listo para predicción
        """
        print("\n" + "="*80)
        print("🎨 INICIANDO ENCODING - DUMIFICACIÓN COMPLETA")
        print("="*80)
        
        # Cargar mapa si se proporciona
        if mapa_categorias_path and not self.mapa_categorias:
            self._cargar_mapa_categorias(mapa_categorias_path)
        
        data = data_limpia.copy()
        
        # ========== FASE 1: TRANSFORMACIONES INICIALES ==========
        print("\n" + "="*80)
        print("FASE 1: TRANSFORMACIONES INICIALES")
        print("="*80)
        
        data = self._transformar_benef_beca(data)
        data = self._transformar_sexo(data)
        
        # ========== FASE 2: ELIMINAR PROGRAMAS FINALIZADOS ==========
        print("\n" + "="*80)
        print("FASE 2: ELIMINAR PROGRAMAS FINALIZADOS")
        print("="*80)
        
        data = self._eliminar_programas_finalizados(data)
        
        # ========== FASE 3: CREAR ESTADO DROPOUT ==========
        print("\n" + "="*80)
        print("FASE 3: CREAR ESTADO DROPOUT")
        print("="*80)
        
        data = self._crear_estado_dropout(data)
        
        # ========== FASE 4: DUMIFICACIÓN PROGRAMA Y SIGLAS ==========
        print("\n" + "="*80)
        print("FASE 4: DUMIFICACIÓN PROGRAMA Y SIGLAS")
        print("="*80)
        
        data = self._dumificar_programa(data)
        data = self._dumificar_siglas_prog(data)
        
        # ========== FASE 5: NORMALIZAR Y DUMIFICAR CIUDAD ==========
        print("\n" + "="*80)
        print("FASE 5: NORMALIZAR Y DUMIFICAR CIUDAD")
        print("="*80)
        
        data = self._normalizar_ciudad(data)
        data = self._limpiar_ciudades_invalidas(data)
        data = self._dumificar_ciudad(data)
        
        # ========== FASE 6: NORMALIZAR Y DUMIFICAR DPTO ==========
        print("\n" + "="*80)
        print("FASE 6: NORMALIZAR Y DUMIFICAR DPTO NACIMIENTO")
        print("="*80)
        
        data = self._normalizar_dpto_nacimiento(data)
        data = self._dumificar_dpto_nacimiento(data)
        
        # ========== FASE 7: ELIMINAR PAÍS NACIMIENTO ==========
        print("\n" + "="*80)
        print("FASE 7: ELIMINAR PAÍS NACIMIENTO")
        print("="*80)
        
        data = self._eliminar_pais_nacimiento(data)
        
        # ========== FASE 8: CODIFICAR SITUACION ACAD ==========
        print("\n" + "="*80)
        print("FASE 8: CODIFICAR SITUACION ACAD (ORDINAL)")
        print("="*80)
        
        data = self._codificar_situacion_acad(data)
        
        # ========== FASE 9: NORMALIZAR CLASES MIN/MAX ==========
        print("\n" + "="*80)
        print("FASE 9: NORMALIZAR CLASE_MIN/MAX_CICLO")
        print("="*80)
        
        data = self._normalizar_clases_ciclo(data)
        
        # ========== FASE 10: MAPEAR CATEGORÍAS Y DUMIFICAR ==========
        print("\n" + "="*80)
        print("FASE 10: MAPEAR CATEGORÍAS Y DUMIFICAR")
        print("="*80)
        
        data = self._mapear_categorias_materias(data)
        data = self._dumificar_cat_clasemax(data)
        data = self._dumificar_cat_clasemin(data)
        
        # ========== FASE 11: DUMIFICAR TIPO ADMISIÓN ==========
        print("\n" + "="*80)
        print("FASE 11: DUMIFICAR TIPO ADMISIÓN")
        print("="*80)
        
        data = self._dumificar_tipo_admision(data)
        
        print("\n" + "="*80)
        print(f"✅ ENCODING COMPLETADO")
        print(f"   • Registros finales: {len(data)}")
        print(f"   • Columnas finales: {len(data.columns)}")
        print(f"   • Lista para predicción")
        print("="*80)
        
        return data
    
    # ============================================================================
    # FASE 1: TRANSFORMACIONES INICIALES
    # ============================================================================
    
    def _transformar_benef_beca(self, data):
        """Transformar Benef. Beca a 0/1"""
        print("\n🔄 Transformando Benef. Beca...")
        
        if "Benef. Beca" in data.columns:
            data["Benef. Beca"] = data["Benef. Beca"].replace({"Y": 1, "N": 0})
            valores_unicos = data["Benef. Beca"].unique()
            print(f"   ✓ Benef. Beca → {valores_unicos}")
        else:
            print("   ⚠️ Columna 'Benef. Beca' no encontrada")
        
        return data
    
    def _transformar_sexo(self, data):
        """Transformar Sexo a 0/1"""
        print("\n🔄 Transformando Sexo...")
        
        if "Sexo" in data.columns:
            data["Sexo"] = data["Sexo"].replace({"M": 1, "F": 0})
            valores_unicos = data["Sexo"].unique()
            print(f"   ✓ Sexo → {valores_unicos}")
        else:
            print("   ⚠️ Columna 'Sexo' no encontrada")
        
        return data
    
    # ============================================================================
    # FASE 2: ELIMINAR PROGRAMAS FINALIZADOS
    # ============================================================================
    
    def _eliminar_programas_finalizados(self, data):
        """Eliminar registros de programas finalizados"""
        print("\n🗑️ Eliminando programas finalizados...")
        
        if "Estado" not in data.columns or "ID" not in data.columns or "Programa" not in data.columns:
            print("   ⚠️ Columnas necesarias no encontradas")
            return data
        
        registros_antes = len(data)
        
        # 1. Para cada (ID, Programa) ver si todos los estados son "Programa Finalizado"
        finalizado_por_programa = (
            data.groupby(['ID', 'Programa'])['Estado']
            .apply(lambda x: (x == 'Programa Finalizado').all())
            .reset_index(name='finalizado_completo')
        )
        
        # 2. Para cada ID ver si todos sus programas están finalizados
        finalizado_por_id = (
            finalizado_por_programa.groupby('ID')['finalizado_completo']
            .all()
            .reset_index(name='id_finalizado_completo')
        )
        
        # 3. Unir info a la base original
        data = data.merge(finalizado_por_programa, on=['ID', 'Programa'])
        data = data.merge(finalizado_por_id, on='ID')
        
        # 4. Eliminar:
        # - Si el ID está completamente finalizado → eliminar todo
        # - Si solo un programa está finalizado pero no todos → eliminar solo ese programa
        data = data[
            ~(data['id_finalizado_completo'] | data['finalizado_completo'])
            | (data['id_finalizado_completo'] == False)
        ]
        
        # 5. Limpiar columnas auxiliares
        data = data.drop(columns=['finalizado_completo', 'id_finalizado_completo'])
        
        registros_despues = len(data)
        print(f"   ✓ Registros: {registros_antes} → {registros_despues} ({registros_antes - registros_despues} eliminados)")
        print(f"   ✓ IDs únicos: {data['ID'].nunique()}")
        
        return data
    
    # ============================================================================
    # FASE 3: CREAR ESTADO DROPOUT
    # ============================================================================
    
    def _crear_estado_dropout(self, data):
        """Crear variable Estado (Dropout) binaria"""
        print("\n🎯 Creando Estado (Dropout)...")
        
        if "Estado" not in data.columns:
            print("   ⚠️ Columna 'Estado' no encontrada")
            return data
        
        mapeo_estado = {
            "Activo en Programa": 0,
            "Suspendido": 1,
            "Permiso": 1,
            "Interrumpido": 1,
            "Expulsado": 1,
            "Cancelado": 1
        }
        
        data["Estado (Dropout)"] = data["Estado"].map(mapeo_estado)
        
        valores_unicos = data["Estado (Dropout)"].unique()
        print(f"   ✓ Estado (Dropout) creado: {valores_unicos}")
        
        # Distribución
        distribucion = data["Estado (Dropout)"].value_counts()
        print(f"   ✓ Distribución:")
        for valor, count in distribucion.items():
            print(f"      - {valor}: {count} registros")
        
        return data
    
    # ============================================================================
    # FASE 4: DUMIFICACIÓN PROGRAMA Y SIGLAS
    # ============================================================================
    
    def _dumificar_programa(self, data):
        """Crear dummies para Programa con prefijo p_"""
        print("\n🎨 Dumificando Programa...")
        
        if "Programa" not in data.columns:
            print("   ⚠️ Columna 'Programa' no encontrada")
            return data
        
        n_programas = data['Programa'].nunique()
        dummies = pd.get_dummies(data['Programa'], prefix='p')
        data = pd.concat([data, dummies], axis=1)
        data = data.drop('Programa', axis=1)
        
        print(f"   ✓ {n_programas} programas → {len(dummies.columns)} dummies (p_*)")
        
        return data
    
    def _dumificar_siglas_prog(self, data):
        """Crear dummies para Siglas Prog con prefijo s_"""
        print("\n🎨 Dumificando Siglas Prog...")
        
        if "Siglas Prog" not in data.columns:
            print("   ⚠️ Columna 'Siglas Prog' no encontrada")
            return data
        
        n_siglas = data['Siglas Prog'].nunique()
        dummies = pd.get_dummies(data['Siglas Prog'], prefix='s')
        data = pd.concat([data, dummies], axis=1)
        data = data.drop('Siglas Prog', axis=1)
        
        print(f"   ✓ {n_siglas} siglas → {len(dummies.columns)} dummies (s_*)")
        
        return data
    
    # ============================================================================
    # FASE 5: NORMALIZAR Y DUMIFICAR CIUDAD
    # ============================================================================
    
    def _normalizar_texto(self, texto):
        """Normaliza texto: elimina espacios extra, estandariza capitalización"""
        if pd.isna(texto):
            return texto
        texto = str(texto).strip().title()
        return texto
    
    def _normalizar_ciudad(self, data):
        """Normalizar Ciudad (Dirección)"""
        print("\n🗺️ Normalizando Ciudad (Dirección)...")
        
        if "Ciudad (Dirección)" not in data.columns:
            print("   ⚠️ Columna 'Ciudad (Dirección)' no encontrada")
            return data
        
        # Diccionario de reemplazos
        reemplazos_ciudades = {
            'Bogota': 'Bogotá D.C.', 'Bogotá': 'Bogotá D.C.', 'Bog': 'Bogotá D.C.',
            'Bogotád.C.': 'Bogotá D.C.', ' Bogotá D.C.': 'Bogotá D.C.',
            'BOGOTÁD.C.': 'Bogotá D.C.', 'BOGOTA': 'Bogotá D.C.', 'BOGOTÁ': 'Bogotá D.C.',
            'Cajica': 'Cajicá', 'Chia': 'Chía', 'Zipaquira': 'Zipaquirá',
            'Santiago Cali': 'Cali', 'Medellin': 'Medellín',
            'BUCARAMANGA': 'Bucaramanga'
        }
        
        ciudades_antes = data["Ciudad (Dirección)"].nunique()
        
        # Normalizar texto
        data["Ciudad (Dirección)"] = data["Ciudad (Dirección)"].apply(self._normalizar_texto)
        
        # Aplicar reemplazos
        data["Ciudad (Dirección)"] = data["Ciudad (Dirección)"].replace(reemplazos_ciudades)
        
        ciudades_despues = data["Ciudad (Dirección)"].nunique()
        
        print(f"   ✓ Ciudades: {ciudades_antes} → {ciudades_despues} ({ciudades_antes - ciudades_despues} consolidadas)")
        
        return data
    
    def _limpiar_ciudades_invalidas(self, data):
        """Limpiar ciudades inválidas (siglas, códigos) → 'Otro'"""
        print("\n🧹 Limpiando ciudades inválidas...")
        
        if "Ciudad (Dirección)" not in data.columns:
            return data
        
        # Reemplazar valores numéricos
        data["Ciudad (Dirección)"] = data["Ciudad (Dirección)"].apply(
            lambda x: "Otro" if isinstance(x, (int, float)) or (isinstance(x, str) and x.strip().isdigit()) else x
        )
        
        # Lista de valores a convertir a "Otro"
        valores_a_otro = [
            'Rm', 'Ma', 'Ar', 'La', 'Lp', 'Zu', 'Bo', 'An', 'Po', 'Pr', 'Ct',
            'Co', 'Sp', 'Ta', 'Lo', 'Sc', 'Nsw', 'Gt Lon', 'Ccs', 'Qroo',
            '92500 Rueil-Malmaison', 'Roma Rm', 'Otro'
        ]
        
        def debe_ser_otro(valor):
            if pd.isna(valor):
                return False
            valor_str = str(valor).strip()
            
            if valor_str in valores_a_otro:
                return True
            
            valor_sin_espacios = valor_str.replace(' ', '')
            if len(valor_sin_espacios) <= 2:
                return True
            
            if re.match(r'^[A-Z]{2,3}$', valor_sin_espacios):
                return True
            
            if re.match(r'^\d+', valor_str):
                return True
            
            return False
        
        invalidos_antes = data["Ciudad (Dirección)"].apply(debe_ser_otro).sum()
        data["Ciudad (Dirección)"] = data["Ciudad (Dirección)"].apply(
            lambda x: 'Otro' if debe_ser_otro(x) else x
        )
        
        print(f"   ✓ {invalidos_antes} ciudades inválidas → 'Otro'")
        
        return data
    
    def _dumificar_ciudad(self, data):
        """Crear dummies para Ciudad y mantener solo Bogotá D.C."""
        print("\n🎨 Dumificando Ciudad (Dirección)...")
        
        if "Ciudad (Dirección)" not in data.columns:
            return data
        
        n_ciudades = data['Ciudad (Dirección)'].nunique()
        dummies = pd.get_dummies(data['Ciudad (Dirección)'], prefix='cd')
        data = pd.concat([data, dummies], axis=1)
        data = data.drop('Ciudad (Dirección)', axis=1)
        
        print(f"   ✓ {n_ciudades} ciudades → {len(dummies.columns)} dummies (cd_*)")
        
        # Mantener solo Bogotá D.C.
        cols_cd = data.filter(regex="^cd_").columns
        cols_a_eliminar = [c for c in cols_cd if c != "cd_Bogotá D.C."]
        
        if cols_a_eliminar:
            data = data.drop(columns=cols_a_eliminar)
            print(f"   ✓ Mantenida solo 'cd_Bogotá D.C.', eliminadas {len(cols_a_eliminar)} otras")
        
        return data
    
    # ============================================================================
    # FASE 6: NORMALIZAR Y DUMIFICAR DPTO
    # ============================================================================
    
    def _normalizar_dpto_nacimiento(self, data):
        """Normalizar Dpto Nacimiento (separa colombianos, extranjeros, otros)"""
        print("\n🗺️ Normalizando Dpto Nacimiento...")
        
        if "Dpto Nacimiento" not in data.columns:
            print("   ⚠️ Columna 'Dpto Nacimiento' no encontrada")
            return data
        
        # Departamentos colombianos válidos
        deptos_colombia = {
            'BOG', 'CUN', 'ANT', 'ATL', 'BOL', 'BOY', 'CAL', 'CAQ', 'CAS', 'CAU',
            'CES', 'CHO', 'COR', 'HUI', 'LAG', 'MAG', 'MET', 'NAR', 'NSA', 'PUT',
            'QUI', 'RIS', 'SAN', 'STD', 'SUC', 'TOL', 'VAL', 'VAU', 'ARA', 'AMA',
            'GAV', 'GAI', 'VIC'
        }
        
        # Países extranjeros
        paises_extranjeros = {
            'USA', 'MEX', 'CAN', 'NY', 'CA', 'TX', 'FL', 'LA', 'MI', 'MO',
            'MA', 'IL', 'WA', 'MT', 'PA', 'NC', 'NE', 'BE', 'MN',
            'GTM', 'SLV', 'HND', 'NIC', 'CRI', 'PAN', 'DOM', 'CUB', 'HTI', 'PRI', 'ABW', 'BB',
            'ECU', 'PER', 'BRA', 'CHL', 'ARG', 'URY', 'PRY', 'BOL', 'VEN',
            'RJ', 'MG', 'SP', 'RS', 'BA', 'PE', 'DF', 'SU', 'BL',
            'ESP', 'FRA', 'GBR', 'ITA', 'DEU', 'NLD', 'PRT', 'CHE', 'SWE',
            'AUT', 'GRC', 'CZE', 'RUS', 'MDA', 'NL', 'RM', 'BCN', 'GT LON',
            'HE', 'GE', 'BG', 'BY', 'NW', 'NAP', 'SH', 'CF',
            'KOR', 'CHN', 'JPN', 'IDN', 'THA', 'VNM', 'PRK', 'SAU', 'IRN', 'SGP', 'UZB',
            'TZA', 'TGO', 'KEN', 'GAB', 'COG', 'COD', 'DZA', 'MOR',
            'AUS', 'ZH', 'VIC', 'MERSYD', 'BRIST',
            'EMEX', 'JAL', 'MICH', 'VER', 'DGO', 'BCS', 'FA', 'Z1', 'TA', 'CE',
            'ON', 'QC', 'AM', 'AN', 'AR', 'BO', 'CO', 'HH', 'LP', 'ME', 'PI',
            'PR', 'SC', 'SN', 'VA', 'ZU', 'CCS', 'PHL'
        }
        
        def clasificar_valor(valor):
            if pd.isna(valor):
                return valor
            
            valor_str = str(valor).strip().upper()
            
            if valor_str in deptos_colombia:
                return valor_str
            
            if valor_str in paises_extranjeros:
                return 'Ext'
            
            if valor_str == 'COL':
                return 'Otro'
            
            if valor_str == 'OTRO':
                return 'Otro'
            
            return 'Otro'
        
        valores_antes = data["Dpto Nacimiento"].nunique()
        data["Dpto Nacimiento"] = data["Dpto Nacimiento"].apply(clasificar_valor)
        valores_despues = data["Dpto Nacimiento"].nunique()
        
        print(f"   ✓ Dptos: {valores_antes} → {valores_despues}")
        print(f"      - Colombianos: {data['Dpto Nacimiento'].isin(deptos_colombia).sum()}")
        print(f"      - Extranjeros (Ext): {(data['Dpto Nacimiento'] == 'Ext').sum()}")
        print(f"      - Otros: {(data['Dpto Nacimiento'] == 'Otro').sum()}")
        
        return data
    
    def _dumificar_dpto_nacimiento(self, data):
        """Crear dummies para Dpto Nacimiento (sin Otro y Ext)"""
        print("\n🎨 Dumificando Dpto Nacimiento...")
        
        if "Dpto Nacimiento" not in data.columns:
            return data
        
        n_dptos = data['Dpto Nacimiento'].nunique()
        dummies = pd.get_dummies(data['Dpto Nacimiento'], prefix='dn')
        data = pd.concat([data, dummies], axis=1)
        
        # Eliminar columna original y dummies de Otro y Ext
        cols_a_eliminar = ['Dpto Nacimiento']
        if 'dn_Otro' in data.columns:
            cols_a_eliminar.append('dn_Otro')
        if 'dn_Ext' in data.columns:
            cols_a_eliminar.append('dn_Ext')
        
        data = data.drop(columns=cols_a_eliminar, errors='ignore')
        
        print(f"   ✓ {n_dptos} departamentos → {len(dummies.columns)} dummies (dn_*)")
        print(f"   ✓ Eliminadas: dn_Otro, dn_Ext")
        
        return data
    
    # ============================================================================
    # FASE 7: ELIMINAR PAÍS NACIMIENTO
    # ============================================================================
    
    def _eliminar_pais_nacimiento(self, data):
        """Eliminar columna País Nacimiento"""
        print("\n🗑️ Eliminando País Nacimiento...")
        
        if "País Nacimiento" in data.columns:
            data = data.drop(columns=['País Nacimiento'])
            print("   ✓ País Nacimiento eliminado")
        else:
            print("   ⚠️ Columna 'País Nacimiento' no encontrada")
        
        return data
    
    # ============================================================================
    # FASE 8: CODIFICAR SITUACION ACAD
    # ============================================================================
    
    def _codificar_situacion_acad(self, data):
        """Codificar Situacion Acad con OrdinalEncoder"""
        print("\n🔢 Codificando Situacion Acad (ordinal)...")
        
        if "Situacion Acad" not in data.columns:
            print("   ⚠️ Columna 'Situacion Acad' no encontrada")
            return data
        
        categorias_ordenadas = [['Normal', 'Primera Prueba', 'Segunda Prueba', 'Excluido']]
        encoder = OrdinalEncoder(categories=categorias_ordenadas)
        
        data['Situacion Acad Cod'] = encoder.fit_transform(data[['Situacion Acad']])
        data['Situacion Acad Cod'] = data['Situacion Acad Cod'].astype(int)
        
        # Mostrar mapeo
        mapeo = data[['Situacion Acad', 'Situacion Acad Cod']].drop_duplicates().sort_values('Situacion Acad Cod')
        print("   ✓ Mapeo aplicado:")
        for _, row in mapeo.iterrows():
            print(f"      {row['Situacion Acad']} → {row['Situacion Acad Cod']}")
        
        # Distribución
        distribucion = data['Situacion Acad Cod'].value_counts().sort_index()
        print(f"   ✓ Distribución: {distribucion.to_dict()}")
        
        return data
    
    # ============================================================================
    # FASE 9: NORMALIZAR CLASES MIN/MAX
    # ============================================================================
    
    def _normalizar_clases_ciclo(self, data):
        """Normalizar Clase_Min_Ciclo y Clase_Max_Ciclo a Title Case"""
        print("\n📝 Normalizando Clase_Min_Ciclo y Clase_Max_Ciclo...")
        
        if 'Clase_Min_Ciclo' in data.columns:
            valores_antes = data['Clase_Min_Ciclo'].nunique()
            data['Clase_Min_Ciclo'] = data['Clase_Min_Ciclo'].str.title()
            valores_despues = data['Clase_Min_Ciclo'].nunique()
            print(f"   ✓ Clase_Min_Ciclo: {valores_antes} → {valores_despues} valores únicos")
        
        if 'Clase_Max_Ciclo' in data.columns:
            valores_antes = data['Clase_Max_Ciclo'].nunique()
            data['Clase_Max_Ciclo'] = data['Clase_Max_Ciclo'].str.title()
            valores_despues = data['Clase_Max_Ciclo'].nunique()
            print(f"   ✓ Clase_Max_Ciclo: {valores_antes} → {valores_despues} valores únicos")
        
        return data
    
    # ============================================================================
    # FASE 10: MAPEAR CATEGORÍAS Y DUMIFICAR
    # ============================================================================
    
    def _mapear_categorias_materias(self, data):
        """Mapear Clase_Min/Max_Ciclo a categorías"""
        print("\n🗂️ Mapeando categorías de materias...")
        
        if not self.mapa_categorias:
            print("   ⚠️ Mapa de categorías no cargado")
            return data
        
        if 'Clase_Max_Ciclo' in data.columns:
            data['Cat_ClaseMax'] = data['Clase_Max_Ciclo'].map(self.mapa_categorias)
            sin_mapeo = data['Cat_ClaseMax'].isna().sum()
            print(f"   ✓ Cat_ClaseMax creada ({sin_mapeo} sin categoría)")
        
        if 'Clase_Min_Ciclo' in data.columns:
            data['Cat_ClaseMin'] = data['Clase_Min_Ciclo'].map(self.mapa_categorias)
            sin_mapeo = data['Cat_ClaseMin'].isna().sum()
            print(f"   ✓ Cat_ClaseMin creada ({sin_mapeo} sin categoría)")
        
        # Eliminar columnas originales
        cols_drop = ['Clase_Max_Ciclo', 'ID_Max_Ciclo', 'Clase_Min_Ciclo', 'ID_Min_Ciclo']
        cols_found = [c for c in cols_drop if c in data.columns]
        if cols_found:
            data = data.drop(columns=cols_found)
            print(f"   ✓ Eliminadas: {cols_found}")
        
        return data
    
    def _dumificar_cat_clasemax(self, data):
        """Crear dummies para Cat_ClaseMax"""
        print("\n🎨 Dumificando Cat_ClaseMax...")
        
        if "Cat_ClaseMax" not in data.columns:
            print("   ⚠️ Columna 'Cat_ClaseMax' no encontrada")
            return data
        
        n_cats = data['Cat_ClaseMax'].nunique()
        dummies = pd.get_dummies(data['Cat_ClaseMax'], prefix='ccmax')
        data = pd.concat([data, dummies], axis=1)
        data = data.drop('Cat_ClaseMax', axis=1)
        
        print(f"   ✓ {n_cats} categorías → {len(dummies.columns)} dummies (ccmax_*)")
        
        return data
    
    def _dumificar_cat_clasemin(self, data):
        """Crear dummies para Cat_ClaseMin"""
        print("\n🎨 Dumificando Cat_ClaseMin...")
        
        if "Cat_ClaseMin" not in data.columns:
            print("   ⚠️ Columna 'Cat_ClaseMin' no encontrada")
            return data
        
        n_cats = data['Cat_ClaseMin'].nunique()
        dummies = pd.get_dummies(data['Cat_ClaseMin'], prefix='ccmin')
        data = pd.concat([data, dummies], axis=1)
        data = data.drop('Cat_ClaseMin', axis=1)
        
        print(f"   ✓ {n_cats} categorías → {len(dummies.columns)} dummies (ccmin_*)")
        
        return data
    
    # ============================================================================
    # FASE 11: DUMIFICAR TIPO ADMISIÓN
    # ============================================================================
    
    def _dumificar_tipo_admision(self, data):
        """Crear dummies para Tipo Admisión"""
        print("\n🎨 Dumificando Tipo Admisión...")
        
        if "Tipo Admisión" not in data.columns:
            print("   ⚠️ Columna 'Tipo Admisión' no encontrada")
            return data
        
        n_tipos = data['Tipo Admisión'].nunique()
        dummies = pd.get_dummies(data['Tipo Admisión'], prefix='ta')
        data = pd.concat([data, dummies], axis=1)
        data = data.drop('Tipo Admisión', axis=1)
        
        print(f"   ✓ {n_tipos} tipos → {len(dummies.columns)} dummies (ta_*)")
        
        return data


# =============================================================================
# FUNCIÓN PRINCIPAL PARA STREAMLIT
# =============================================================================

def procesar_encoding_completo(data_limpia_df, mapa_categorias_path):
    """
    Función para usar en Streamlit que procesa y retorna DataFrame codificado
    
    Args:
        data_limpia_df: DataFrame resultado del procesador de limpieza
        mapa_categorias_path: Ruta al Excel con categorías de materias
        
    Returns:
        DataFrame codificado listo para predicción
    """
    procesador = DataProcessorEncoding(mapa_categorias_path)
    data_encoded = procesador.procesar(data_limpia_df)
    return data_encoded


# =============================================================================
# EJEMPLO DE USO
# =============================================================================

if __name__ == "__main__":
    procesador = DataProcessorEncoding("Libro1.xlsx")
    # data_encoded = procesador.procesar(data_limpia)
    print("\n✅ Procesador de encoding listo")
