import streamlit as st
import time
from modules import services, ui, views

# Configura página e CSS
ui.configurar_pagina()

ADMINS = ["admin"] 
def main():
    usuario_logado = views.tela_login(st.secrets["passwords"])
    
    with st.sidebar:
        st.write(f"👤 **{usuario_logado}**")
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            st.toast("Atualizado!", icon="✅"); time.sleep(0.5); st.rerun()
        st.divider()
        if st.button("Sair"):
            services.get_manager().delete("usuario_coleta")
            if 'usuario_logado_temp' in st.session_state: del st.session_state['usuario_logado_temp']
            st.rerun()
        st.divider()

    if usuario_logado in ADMINS:
        modo = st.sidebar.radio("Menu Admin", ["Produção", "Painel Admin"])
        if modo == "Painel Admin": 
            views.tela_admin_area()
        else: 
            views.tela_producao(usuario_logado)
    else:
        views.tela_producao(usuario_logado)

if __name__ == "__main__":
    main()