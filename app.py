import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import uuid
import time
import extra_streamlit_components as stx

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="Sistema Coleta Links", layout="wide", page_icon="🔗")

# --- DEFINA AQUI QUEM SÃO OS ADMINS ---
# Coloque exatamente o nome do usuário que está no secrets.toml
ADMINS = ["admin", "Diego", "Eduardo"] 

# --- 1. CONEXÃO E CACHE ---
@st.cache_resource
def get_manager():
    return stx.CookieManager()

@st.cache_resource
def get_client_google():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

# --- 2. FUNÇÕES DE LEITURA ---
def carregar_projetos_ativos():
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("projetos")
    df = pd.DataFrame(ws.get_all_records())
    if not df.empty:
        return df[df['status'] == 'Ativo']
    return df

def carregar_lotes_do_projeto(id_projeto):
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("controle_lotes")
    df = pd.DataFrame(ws.get_all_records())
    if not df.empty:
        df['id_projeto'] = df['id_projeto'].astype(str)
        return df[df['id_projeto'] == str(id_projeto)]
    return df

def carregar_dados_lote(id_projeto, numero_lote):
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
    dados = ws.get_all_records()
    df = pd.DataFrame(dados)
    if not df.empty:
        df['id_projeto'] = df['id_projeto'].astype(str)
        df['lote'] = df['lote'].astype(str)
        filtro = df[
            (df['id_projeto'] == str(id_projeto)) & 
            (df['lote'] == str(numero_lote))
        ]
        return filtro
    return df

# --- 3. FUNÇÕES DE GRAVAÇÃO ---
def reservar_lote(id_projeto, numero_lote, usuario):
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("controle_lotes")
    registros = ws.get_all_records()
    for i, row in enumerate(registros):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            linha = i + 2
            if row['status'] == "Livre" or (row['status'] == "Em Andamento" and row['usuario'] == usuario):
                ws.update_cell(linha, 3, "Em Andamento")
                ws.update_cell(linha, 4, usuario)
                return True
    return False

def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_dados = ss.worksheet("dados_brutos")
    ws_lotes = ss.worksheet("controle_lotes")
    
    todos_dados = ws_dados.get_all_records()
    batch_updates = []
    mapa_linhas = {}
    
    # Mapeia linhas
    for i, row in enumerate(todos_dados):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            mapa_linhas[str(row['ean'])] = i + 2
            
    # Prepara update dos links
    for index, row in df_editado.iterrows():
        linha_sheet = mapa_linhas.get(str(row['ean']))
        if linha_sheet:
            novo_link = row['link']
            batch_updates.append({
                'range': f'E{linha_sheet}', # Coluna E é Link
                'values': [[novo_link]]
            })
            
    if batch_updates:
        ws_dados.batch_update(batch_updates)
        
    # Atualiza Status Lote
    total_links = df_editado['link'].replace('', pd.NA).isna().sum()
    total_preenchidos = len(df_editado) - total_links
    progresso_str = f"{total_preenchidos}/{len(df_editado)}"
    
    todos_lotes = ws_lotes.get_all_records()
    for i, row in enumerate(todos_lotes):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            linha_lote = i + 2
            ws_lotes.update_cell(linha_lote, 5, progresso_str)
            if concluir:
                ws_lotes.update_cell(linha_lote, 3, "Concluído")
            break
    return True

def processar_upload_lotes(df, nome_arquivo):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_projetos = ss.worksheet("projetos")
    ws_lotes = ss.worksheet("controle_lotes")
    ws_dados = ss.worksheet("dados_brutos")
    
    id_projeto = str(uuid.uuid4())[:8]
    data_hoje = datetime.now().strftime("%d/%m/%Y")
    total_linhas = len(df)
    total_lotes = (total_linhas // 100) + (1 if total_linhas % 100 > 0 else 0)
    
    lista_dados = []
    lista_lotes = []
    
    for i in range(total_lotes):
        num_lote = i + 1
        inicio, fim = i * 100, (i + 1) * 100
        df_lote = df.iloc[inicio:fim]
        for _, row in df_lote.iterrows():
            ean = row.get('ean', row.iloc[1] if len(row)>1 else '')
            desc = row.get('descricao', row.iloc[0] if len(row)>0 else '')
            lista_dados.append([id_projeto, num_lote, str(ean), desc, ""])
        lista_lotes.append([id_projeto, num_lote, "Livre", "", f"0/{len(df_lote)}"])

    ws_projetos.append_row([id_projeto, nome_arquivo, data_hoje, total_lotes, "Ativo"])
    ws_lotes.append_rows(lista_lotes)
    ws_dados.append_rows(lista_dados)
    return id_projeto, total_lotes

# --- 4. TELAS ---
def tela_login():
    cookie_manager = get_manager()
    cookie_usuario = cookie_manager.get(cookie="usuario_coleta")
    if cookie_usuario: return cookie_usuario

    st.title("🔒 Acesso Restrito - Coleta")
    try: usuarios = st.secrets["passwords"]
    except: st.error("Configure os Secrets."); st.stop()

    col1, col2 = st.columns([2,1])
    with col1:
        user_input = st.selectbox("Usuário", ["Selecione..."] + list(usuarios.keys()))
        pass_input = st.text_input("Senha", type="password")
        if st.button("Entrar", type="primary"):
            if user_input != "Selecione..." and pass_input == usuarios[user_input]:
                cookie_manager.set("usuario_coleta", user_input, expires_at=datetime.now() + timedelta(days=1))
                time.sleep(1)
                st.rerun()
            else:
                st.error("Senha incorreta.")
    st.stop()

def tela_admin_upload():
    st.markdown("## 📤 Admin: Upload de Projetos")
    st.info("Aqui você sobe a lista de produtos (EAN, Descrição) e o sistema divide em lotes automaticamente.")
    
    arquivo = st.file_uploader("Suba o Excel (Colunas: ean, descricao)", type=["xlsx"])
    if arquivo:
        if st.button("🚀 Processar Lotes", type="primary"):
            try:
                df = pd.read_excel(arquivo)
                df.columns = [str(c).lower().strip() for c in df.columns]
                with st.spinner("Processando e enviando..."):
                    id_proj, qtd = processar_upload_lotes(df, arquivo.name)
                    st.success(f"Projeto {id_proj} criado com {qtd} lotes!")
                    st.balloons()
            except Exception as e:
                st.error(f"Erro: {e}")

def tela_producao(usuario):
    st.title(f"🏭 Área de Coleta | {usuario}")
    
    projetos = carregar_projetos_ativos()
    if projetos.empty:
        st.info("Nenhum projeto ativo no momento.")
        return

    proj_dict = {f"{row['nome']} ({row['data']})": row['id'] for _, row in projetos.iterrows()}
    nome_proj = st.selectbox("Selecione o Projeto:", ["Selecione..."] + list(proj_dict.keys()))
    
    if nome_proj == "Selecione...": st.stop()
    id_proj = proj_dict[nome_proj]
    
    df_lotes = carregar_lotes_do_projeto(id_proj)
    if df_lotes.empty:
        st.warning("Projeto sem lotes.")
        return

    meus_lotes = df_lotes[(df_lotes['status'] == 'Em Andamento') & (df_lotes['usuario'] == usuario)]
    lotes_livres = df_lotes[df_lotes['status'] == 'Livre']
    
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("### 🏃 Meus Lotes")
        if not meus_lotes.empty:
            lote_radio = st.radio("Continuar:", meus_lotes['lote'].astype(str).unique(), key="radio_meus")
            if st.button("▶️ Retomar"):
                st.session_state['lote_trabalho'] = lote_radio
                st.rerun()
        else: st.write("Nenhum.")

    with col_b:
        st.markdown("### 🆕 Pegar Novo")
        if not lotes_livres.empty:
            lote_novo = st.selectbox("Disponíveis:", lotes_livres['lote'].astype(str).unique())
            if st.button("🙋 Pegar"):
                if reservar_lote(id_proj, lote_novo, usuario):
                    st.session_state['lote_trabalho'] = lote_novo
                    st.success("Reservado!"); time.sleep(1); st.rerun()
                else: st.error("Alguém pegou antes!")
        else: st.info("Sem lotes livres.")

    st.divider()

    if 'lote_trabalho' in st.session_state:
        num_lote = st.session_state['lote_trabalho']
        st.markdown(f"## 📝 Lote {num_lote}")
        df_dados = carregar_dados_lote(id_proj, num_lote)
        
        edited_df = st.data_editor(
            df_dados,
            column_config={
                "id_projeto": None, "lote": None,
                "ean": st.column_config.TextColumn("EAN", disabled=True),
                "descricao": st.column_config.TextColumn("Descrição", disabled=True, width="medium"),
                "link": st.column_config.LinkColumn("Link (Cole Aqui)", validate="^https?://", width="large")
            },
            hide_index=True, use_container_width=True, num_rows="fixed", height=500
        )
        
        c1, c2 = st.columns(2)
        if c1.button("💾 Salvar Parcial"):
            salvar_progresso_lote(edited_df, id_proj, num_lote, False)
            st.toast("Salvo!")
        
        if c2.button("✅ Finalizar Lote"):
            vazios = edited_df['link'].replace('', pd.NA).isna().sum()
            if vazios > 0:
                st.warning(f"Faltam {vazios} links.")
                if st.checkbox("Finalizar mesmo assim"):
                    salvar_progresso_lote(edited_df, id_proj, num_lote, True)
                    del st.session_state['lote_trabalho']
                    st.balloons(); time.sleep(1); st.rerun()
            else:
                salvar_progresso_lote(edited_df, id_proj, num_lote, True)
                del st.session_state['lote_trabalho']
                st.balloons(); time.sleep(1); st.rerun()

# --- MAIN COM ROTEAMENTO ---
def main():
    usuario_logado = tela_login()
    
    with st.sidebar:
        st.write(f"👤 **{usuario_logado}**")
        if st.button("Sair"):
            get_manager().delete("usuario_coleta")
            st.rerun()
        st.divider()

    # --- LÓGICA DE SEPARAÇÃO ---
    if usuario_logado in ADMINS:
        # Se for Admin, mostra menu para escolher
        modo = st.sidebar.radio("Navegação Admin", ["Produção", "Upload Admin"])
        if modo == "Upload Admin":
            tela_admin_upload()
        else:
            tela_producao(usuario_logado)
    else:
        # Se for Estagiário, nem mostra menu, vai direto pra produção
        tela_producao(usuario_logado)

if __name__ == "__main__":
    main()
