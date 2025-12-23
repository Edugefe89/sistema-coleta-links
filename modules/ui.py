import streamlit as st

def render_header_lote(num_lote, info_site, info_cep, info_end):
    st.markdown(f"""
    <div style="background-color: #262730; padding: 15px; border-radius: 10px; margin-bottom: 20px; border: 1px solid #FFD700;">
        <h4 style="margin: 0; color: #FFD700; margin-bottom: 10px;">📍 Dados de Entrega / Contexto (Lote {num_lote})</h4>
        <div style="display: flex; gap: 30px; flex-wrap: wrap; font-size: 16px; color: #FFD700;">
            <div><b style="color: #FFF;">Site:</b> {info_site}</div>
            <div><b style="color: #FFF;">CEP:</b> {info_cep}</div>
            <div><b style="color: #FFF;">Endereço:</b> {info_end}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def configurar_pagina():
    st.set_page_config(page_title="Sistema Coleta Links", layout="wide", page_icon="🔗")