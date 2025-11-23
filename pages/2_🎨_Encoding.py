"""
Página de Encoding - Transformación de base limpia a base codificada
"""

import streamlit as st
from SECCION_ENCODING import seccion_encoding

st.set_page_config(
    page_title="Encoding de Datos",
    page_icon="🎨",
    layout="wide"
)

seccion_encoding()
