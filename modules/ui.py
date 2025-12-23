import streamlit as st

def configurar_pagina():
    st.set_page_config(page_title="Sistema Coleta Links", layout="wide", page_icon="🔗")
    
    # CSS para deixar com cara de sistema profissional (SaaS)
    st.markdown("""
        <style>
        .stButton button {
            width: 100%;
            border-radius: 8px;
            font-weight: bold;
            height: 3em;
        }
        div[data-testid="stExpander"] {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            background-color: #f9f9f9;
        }
        /* Ajuste para modo escuro/claro automático */
        @media (prefers-color-scheme: dark) {
            div[data-testid="stExpander"] {
                background-color: #262730;
                border: 1px solid #444;
            }
        }
        </style>
    """, unsafe_allow_html=True)

def render_header_lote(num_lote, info_site, info_cep, info_end):
    # Card visual com sombra e borda dourada
    st.markdown(f"""
    <div style="background-color: #262730; padding: 15px; border-radius: 10px; margin-bottom: 20px; border: 1px solid #FFD700; box-shadow: 0 4px 6px rgba(0,0,0,0.3);">
        <h4 style="margin: 0; color: #FFD700; margin-bottom: 10px; font-family: sans-serif;">📍 Contexto de Entrega (Lote {num_lote})</h4>
        <div style="display: flex; gap: 30px; flex-wrap: wrap; font-size: 15px; color: #FFD700;">
            <div><b style="color: #FFF;">Site:</b> {info_site}</div>
            <div><b style="color: #FFF;">CEP:</b> {info_cep}</div>
            <div><b style="color: #FFF;">Endereço:</b> {info_end}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)