import streamlit as st
import time
from modules import services, ui, views

# Configuração inicial e CSS
ui.configurar_pagina()

ADMINS = ["admin"] 

def main():
    # Tela de Login
    usuario = views.tela_login(st.secrets["passwords"])
    
    # Sidebar
    with st.sidebar:
        st.write(f"👤 **{usuario}**")
        if st.button("🔄 Atualizar"):
            st.cache_data.clear(); st.toast("Atualizado!"); time.sleep(0.5); st.rerun()
        st.divider()
        if st.button("Sair"):
            services.get_manager().delete("usuario_coleta")
            if 'usuario_logado_temp' in st.session_state: del st.session_state['usuario_logado_temp']
            st.rerun()

    # Roteamento
    if usuario in ADMINS:
        modo = st.sidebar.radio("Navegação", ["Produção", "Admin"])
        if modo == "Admin": views.tela_admin()
        else: views.tela_producao(usuario)
    else:
        views.tela_producao(usuario)

if __name__ == "__main__":
    main()