import streamlit as st
import time
from datetime import timedelta
from modules import services, views

# Configuração da Página deve ser a primeira linha
st.set_page_config(layout="wide", page_title="Sistema Coleta")

def main():
    # --- PONTO CRÍTICO: Criar o CookieManager APENAS UMA VEZ AQUI ---
    cm = services.get_manager()
    time.sleep(0.1) # Pequeno delay técnico
    
    # Verifica se já está logado na sessão (Memória RAM)
    if 'usuario_logado_temp' not in st.session_state:
        # Tenta recuperar via Cookie (Navegador)
        c_usr = cm.get("usuario_coleta")
        if c_usr:
            st.session_state['usuario_logado_temp'] = c_usr
    
    # Se ainda não estiver logado, chama a View de Login
    if 'usuario_logado_temp' not in st.session_state:
        try:
            senhas = st.secrets["passwords"]
        except:
            st.error("Configure as senhas no .streamlit/secrets.toml")
            st.stop()
        
        # --- AQUI ESTÁ A CORREÇÃO: Passamos 'cm' para a view ---
        views.tela_login(senhas, cm)
        return

    # --- USUÁRIO LOGADO ---
    usuario = st.session_state['usuario_logado_temp']

    # Sidebar com controles
    with st.sidebar:
        st.write(f"👤 **{usuario}**")
        
        if st.button("🔄 Atualizar Tela"):
            st.rerun()

        st.divider()
        
        if st.button("Sair"):
            try:
                # Usa o mesmo 'cm' criado lá em cima para deletar
                cm.delete("usuario_coleta")
            except KeyError:
                pass 
            except Exception as e:
                print(f"Aviso logout: {e}")
            
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