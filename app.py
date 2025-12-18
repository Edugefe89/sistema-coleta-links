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

# --- DEFINA AQUI QUEM SÃO OS ADMINS (Quem pode ver a tela de Upload) ---
# Deve ser exatamente igual ao nome que está no secrets.toml
ADMINS = ["admin"] 

# --- 1. CONEXÃO E CACHE ---
def get_manager():
    return stx.CookieManager()

@st.cache_resource
def get_client_google():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        # Pega as credenciais do bloco [connections.gsheets]
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro de Conexão Google: {e}")
        return None

# --- 2. FUNÇÕES DE LEITURA (DO BANCO) ---
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

# --- 3. FUNÇÕES DE GRAVAÇÃO (NO BANCO) ---
def reservar_lote(id_projeto, numero_lote, usuario):
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("controle_lotes")
    registros = ws.get_all_records()
    
    for i, row in enumerate(registros):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            linha = i + 2 # +2 pois header é 1 e indice começa em 0
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
    
    # Mapeia onde está cada EAN na planilha original
    for i, row in enumerate(todos_dados):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            mapa_linhas[str(row['ean'])] = i + 2
            
    # Prepara atualização em massa dos links
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
        
    # Atualiza Status do Lote
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
    
    # Divide em lotes de 100
    for i in range(total_lotes):
        num_lote = i + 1
        inicio, fim = i * 100, (i + 1) * 100
        df_lote = df.iloc[inicio:fim]
        
        for _, row in df_lote.iterrows():
            ean = row.get('ean', row.iloc[1] if len(row)>1 else '')
            desc = row.get('descricao', row.iloc[0] if len(row)>0 else '')
            lista_dados.append([id_projeto, num_lote, str(ean), desc, ""])
            
        lista_lotes.append([id_projeto, num_lote, "Livre", "", f"0/{len(df_lote)}"])

    # Envia tudo de uma vez
    ws_projetos.append_row([id_projeto, nome_arquivo, data_hoje, total_lotes, "Ativo"])
    ws_lotes.append_rows(lista_lotes)
    ws_dados.append_rows(lista_dados)
    
    return id_projeto, total_lotes

# --- 4. TELAS DE INTERFACE ---
def tela_login():
    # 1. VERIFICAÇÃO RÁPIDA (Memória RAM - Instantâneo)
    if 'usuario_logado_temp' in st.session_state:
        return st.session_state['usuario_logado_temp']

    # 2. VERIFICAÇÃO DE COOKIE (Disco - Pode ser lento)
    cookie_manager = get_manager()
    cookie_usuario = cookie_manager.get(cookie="usuario_coleta")
    
    # Se achou cookie, salva na memória para não ler de novo e libera
    if cookie_usuario:
        st.session_state['usuario_logado_temp'] = cookie_usuario
        return cookie_usuario

    st.title("🔒 Acesso Restrito - Coleta")
    
    try: usuarios = st.secrets["passwords"]
    except: st.error("Erro Secrets."); st.stop()

    col1, col2 = st.columns([2,1])
    with col1:
        user_input = st.selectbox("Usuário", ["Selecione..."] + list(usuarios.keys()))
        pass_input = st.text_input("Senha", type="password")
        
        if st.button("Entrar", type="primary"):
            if user_input != "Selecione..." and pass_input == usuarios[user_input]:
                # A. Salva na memória (Isso libera a tela IMEDIATAMENTE na próxima linha)
                st.session_state['usuario_logado_temp'] = user_input
                
                # B. Manda gravar o cookie (Sem esperar confirmação)
                try:
                    cookie_manager.set("usuario_coleta", user_input, expires_at=datetime.now() + timedelta(days=1))
                except:
                    pass # Se falhar o cookie, não tem problema, a sessão segura
                
                # C. Recarrega a página imediatamente
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
                # Normaliza colunas para minúsculo
                df.columns = [str(c).lower().strip() for c in df.columns]
                
                with st.spinner("Processando e enviando para o Google..."):
                    id_proj, qtd = processar_upload_lotes(df, arquivo.name)
                    st.success(f"Projeto criado com sucesso! ID: {id_proj}")
                    st.info(f"Total de Lotes gerados: {qtd}")
                    st.balloons()
            except Exception as e:
                st.error(f"Erro ao processar: {e}")

def tela_producao(usuario):
    st.title(f"🏭 Área de Coleta | {usuario}")
    
    projetos = carregar_projetos_ativos()
    if projetos.empty:
        st.info("Nenhum projeto ativo no momento. Aguarde o Admin fazer upload.")
        return

    # Dropdown de Projetos
    proj_dict = {f"{row['nome']} ({row['data']})": row['id'] for _, row in projetos.iterrows()}
    nome_proj = st.selectbox("Selecione o Projeto:", ["Selecione..."] + list(proj_dict.keys()))
    
    if nome_proj == "Selecione...": st.stop()
    id_proj = proj_dict[nome_proj]
    
    df_lotes = carregar_lotes_do_projeto(id_proj)
    if df_lotes.empty:
        st.warning("Projeto sem lotes gerados.")
        return

    # Filtra lotes do usuário e lotes livres
    meus_lotes = df_lotes[(df_lotes['status'] == 'Em Andamento') & (df_lotes['usuario'] == usuario)]
    lotes_livres = df_lotes[df_lotes['status'] == 'Livre']
    
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("### 🏃 Meus Lotes Atuais")
        if not meus_lotes.empty:
            lote_radio = st.radio("Continuar:", meus_lotes['lote'].astype(str).unique(), key="radio_meus")
            if st.button("▶️ Retomar Trabalho"):
                st.session_state['lote_trabalho'] = lote_radio
                st.rerun()
        else: st.write("Você não tem lotes em andamento.")

    with col_b:
        st.markdown("### 🆕 Pegar Novo Lote")
        if not lotes_livres.empty:
            lote_novo = st.selectbox("Disponíveis:", lotes_livres['lote'].astype(str).unique())
            if st.button("🙋 Pegar Lote"):
                if reservar_lote(id_proj, lote_novo, usuario):
                    st.session_state['lote_trabalho'] = lote_novo
                    st.success("Lote reservado com sucesso!")
                    time.sleep(1)
                    st.rerun()
                else: st.error("Alguém pegou esse lote antes de você. Atualize e tente outro.")
        else: st.info("Não há lotes livres neste projeto.")

    st.divider()

    # --- ÁREA DE TRABALHO ---
    if 'lote_trabalho' in st.session_state:
        num_lote = st.session_state['lote_trabalho']
        st.markdown(f"## 📝 Trabalhando no Lote {num_lote}")
        
        df_dados = carregar_dados_lote(id_proj, num_lote)
        
        # Tabela Editável (Data Editor)
        edited_df = st.data_editor(
            df_dados,
            column_config={
                "id_projeto": None, "lote": None,
                "ean": st.column_config.TextColumn("EAN", disabled=True),
                "descricao": st.column_config.TextColumn("Descrição", disabled=True, width="medium"),
                "link": st.column_config.LinkColumn(
                    "Link (Cole Aqui)", 
                    validate="^https?://", 
                    width="large",
                    help="Cole o link do produto. Deve começar com http://"
                )
            },
            hide_index=True, use_container_width=True, num_rows="fixed", height=500
        )
        
        c1, c2 = st.columns(2)
        
        if c1.button("💾 Salvar Parcial (Continuar depois)"):
            with st.spinner("Salvando no Google Sheets..."):
                salvar_progresso_lote(edited_df, id_proj, num_lote, False)
                st.toast("Progresso salvo!")
        
        if c2.button("✅ Finalizar Lote (Entregar)"):
            vazios = edited_df['link'].replace('', pd.NA).isna().sum()
            if vazios > 0:
                st.warning(f"Atenção: Existem {vazios} produtos sem link.")
                if st.checkbox("Finalizar mesmo assim"):
                    with st.spinner("Finalizando..."):
                        salvar_progresso_lote(edited_df, id_proj, num_lote, True)
                        del st.session_state['lote_trabalho']
                        st.balloons(); time.sleep(1); st.rerun()
            else:
                with st.spinner("Finalizando..."):
                    salvar_progresso_lote(edited_df, id_proj, num_lote, True)
                    del st.session_state['lote_trabalho']
                    st.balloons(); time.sleep(1); st.rerun()

# --- MAIN COM ROTEAMENTO ---
def main():
    usuario_logado = tela_login()
    
    with st.sidebar:
        st.write(f"👤 **{usuario_logado}**")
        
        # --- BOTÃO DE SAIR CORRIGIDO ---
        if st.button("Sair"):
            # 1. Manda apagar o cookie
            get_manager().delete("usuario_coleta")
            
            # 2. Limpa a memória RAM
            if 'usuario_logado_temp' in st.session_state:
                del st.session_state['usuario_logado_temp']
            
            # 3. Mostra mensagem e ESPERA o navegador apagar
            st.toast("Desconectando...", icon="👋")
            time.sleep(0.5) # Pausa tática de meio segundo
            
            # 4. Agora sim recarrega
            st.rerun()
        
        st.divider()

    # --- ROTEAMENTO BLINDADO ---
    if usuario_logado in ADMINS:
        modo = st.sidebar.radio("Menu Admin", ["Produção", "Painel Admin"])
        if modo == "Painel Admin":
            tela_admin_area()
        else:
            tela_producao(usuario_logado)
    else:
        tela_producao(usuario_logado)

if __name__ == "__main__":
    main()
