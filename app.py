import streamlit as st
import time
from modules import services, views

# Configuração da Página deve ser a primeira linha
st.set_page_config(layout="wide", page_title="Sistema Coleta")

def main():
    # Verifica se já está logado na sessão (Memória RAM)
    if 'usuario_logado_temp' not in st.session_state:
        try:
            senhas = st.secrets["passwords"]
        except:
            st.error("Configure as senhas no .streamlit/secrets.toml")
            st.stop()
        
        # Chama a tela de login (sem passar gerenciador de cookies)
        views.tela_login(senhas)
        return

    # --- USUÁRIO LOGADO ---
    usuario = st.session_state['usuario_logado_temp']

    # Sidebar com controles
    with st.sidebar:
        st.write(f"👤 **{usuario}**")
        
        if st.button("🔄 Atualizar Tela"):
            st.rerun()

        st.divider()
        
        # Logout Simples (Apenas limpa a memória)
        if st.button("Sair"):
            if 'usuario_logado_temp' in st.session_state: 
                del st.session_state['usuario_logado_temp']
            st.rerun()

    # --- ROTEAMENTO ---
    if usuario == "admin":
        modo = st.sidebar.radio("Modo:", ["Produção", "Admin"])
        if modo == "Admin":
            views.tela_admin()
        else:
            views.tela_producao(usuario)
    else:
        views.tela_producao(usuario)

if __name__ == "__main__":
    main()