import streamlit as st
import time
from modules import services, views

# Configuração da Página deve ser a primeira linha
st.set_page_config(layout="wide", page_title="Sistema Coleta")

def main():
    # Verifica se já está logado na sessão (Memória RAM)
    if 'usuario_logado_temp' not in st.session_state:
        # Tenta recuperar via Cookie (Navegador)
        cm = services.get_manager()
        time.sleep(0.1) # Pequeno delay técnico para leitura de cookie
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
        
        views.tela_login(senhas)
        return

    # --- USUÁRIO LOGADO ---
    usuario = st.session_state['usuario_logado_temp']

    # Sidebar com controles
    with st.sidebar:
        st.write(f"👤 **{usuario}**")
        
        # Botão útil para forçar recarregamento sem cache
        if st.button("🔄 Atualizar Tela"):
            st.rerun()

        st.divider()
        
        # --- CORREÇÃO DO ERRO KEYERROR ---
        if st.button("Sair"):
            # Tenta apagar o cookie. Se der erro (não existir), ignora e segue.
            try:
                services.get_manager().delete("usuario_coleta")
            except KeyError:
                pass # Cookie já não existe, tudo bem.
            except Exception as e:
                print(f"Aviso logout: {e}")
            
            # Limpa a sessão
            if 'usuario_logado_temp' in st.session_state: 
                del st.session_state['usuario_logado_temp']
            
            # Recarrega a página para voltar ao login
            st.rerun()

    # --- ROTEAMENTO (ADMIN vs USUÁRIO) ---
    if usuario == "admin":
        modo = st.sidebar.radio("Modo:", ["Produção", "Admin"])
        if modo == "Admin":
            views.tela_admin()
        else:
            views.tela_producao(usuario)
    else:
        # Estagiários vão direto para produção
        views.tela_producao(usuario)

if __name__ == "__main__":
    main()