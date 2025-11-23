"""
Página de Ajustes Finales - Dropout corrida y preparación para predicción
"""

import streamlit as st
from SECCION_AJUSTES import seccion_ajustes

st.set_page_config(
    page_title="Ajustes Finales",
    page_icon="🔧",
    layout="wide"
)

seccion_ajustes()
