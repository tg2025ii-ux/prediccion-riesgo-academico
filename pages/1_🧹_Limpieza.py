import streamlit as st
from SECCION_LIMPIEZA import seccion_limpieza

st.set_page_config(
    page_title="Limpieza de Datos",
    page_icon="🧹",
    layout="wide"
)

seccion_limpieza()
