import streamlit as st
import pandas as pd
import math
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import uuid
import time
import extra_streamlit_components as stx
import io
import unicodedata

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="Sistema Coleta Links", layout="wide", page_icon="🔗")

# --- DEFINA AQUI QUEM SÃO OS ADMINS ---
ADMINS = ["admin", "Diego", "Eduardo"] 

# --- 1. CONEXÃO E CACHE ---
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
        st.error(f"Erro de Conexão Google: {e}")
        return None

# --- 2. FUNÇÕES DE LEITURA (COM CACHE) ---

@st.cache_data(ttl=60)
def carregar_projetos_ativos():
    try:
        client = get_client_google()
        ws = client.open("Sistema_Coleta_Links").worksheet("projetos")
        df = pd.DataFrame(ws.get_all_records())
        if not df.empty:
            return df[df['status'] == 'Ativo']
        return df
    except Exception as e:
        # Se der erro de leitura, espera um pouco e retorna vazio para não quebrar
        time.sleep(1)
        return pd.DataFrame()

@st.cache_data(ttl=30)
def carregar_lotes_do_projeto(id_projeto):
    try:
        client = get_client_google()
        ws = client.open("Sistema_Coleta_Links").worksheet("controle_lotes")
        df = pd.DataFrame(ws.get_all_records())
        if not df.empty:
            df['id_projeto'] = df['id_projeto'].astype(str)
            return df[df['id_projeto'] == str(id_projeto)]
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=300) 
def carregar_dados_lote(id_projeto, numero_lote):
    try:
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
    except: return pd.DataFrame()

# --- 3. FUNÇÕES DE PROCESSAMENTO E GRAVAÇÃO ---

def baixar_projeto_completo(id_projeto):
    """Gera o Excel final para download"""
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
    dados = ws.get_all_records()
    df = pd.DataFrame(dados)
    
    # Filtra apenas o projeto selecionado
    df_final = df[df['id_projeto'].astype(str) == str(id_projeto)].copy()
    
    # Remove colunas técnicas
    colunas_remover = ['id_projeto', 'lote']
    df_final = df_final.drop(columns=[c for c in colunas_remover if c in df_final.columns])
    
    # Gera o Excel em memória
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False, sheet_name='Links Coletados')
    
    return output.getvalue()

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

def salvar_alteracao_individual(id_projeto, numero_lote, indice_linha_df, novo_link, df_origem):
    """
    Salva um único link no Google Sheets com proteção contra erros de cota (Rate Limit).
    Tenta 3 vezes com tempo crescente antes de desistir.
    """
    # Pega o EAN da linha editada
    try:
        ean_alvo = str(df_origem.iloc[indice_linha_df]['ean'])
    except:
        return False # Se não achar o EAN, aborta
    
    # Backoff Exponencial: Tenta 3 vezes
    max_tentativas = 3
    
    for tentativa in range(max_tentativas):
        try:
            client = get_client_google()
            ws_dados = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
            
            # Busca a célula do EAN na coluna 3 (EAN)
            # Otimização: find é mais rápido que ler tudo
            cell = ws_dados.find(ean_alvo, in_column=3) 
            
            if cell:
                # Atualiza a célula do link (Coluna 5)
                ws_dados.update_cell(cell.row, 5, novo_link)
                
                # SUCESSO: Limpa o cache para que o F5 traga o dado atualizado e retorna True
                carregar_dados_lote.clear()
                return True
            else:
                # Se não achou o EAN na planilha, algo está errado com a sincronia
                return False
                
        except Exception as e:
            erro_str = str(e).lower()
            # Verifica se é erro de cota (429 ou quota exceeded)
            if "quota" in erro_str or "429" in erro_str or "limit" in erro_str:
                tempo_espera = 2 ** (tentativa + 1) # Espera: 2s, depois 4s, depois 8s
                time.sleep(tempo_espera) 
                # Loop continua...
            else:
                # Se for outro erro grave, loga e sai
                st.error(f"Erro ao salvar: {e}")
                return False

    # Se chegou aqui, esgotou as tentativas
    st.error("⚠️ Rede instável ou Cota do Google excedida. Aguarde alguns segundos antes de tentar novamente.")
    return False

def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_dados = ss.worksheet("dados_brutos")
    ws_lotes = ss.worksheet("controle_lotes")
    
    todos_dados = ws_dados.get_all_records()
    batch_updates = []
    mapa_linhas = {}
    
    # Mapeamento
    for i, row in enumerate(todos_dados):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            mapa_linhas[str(row['ean'])] = i + 2
            
    # Prepara updates
    for index, row in df_editado.iterrows():
        linha_sheet = mapa_linhas.get(str(row['ean']))
        if linha_sheet:
            novo_link = row['link']
            batch_updates.append({
                'range': f'E{linha_sheet}', 
                'values': [[novo_link]]
            })
            
    if batch_updates:
        ws_dados.batch_update(batch_updates)
        
    # Atualiza Status
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
    
    # LIMPA O CACHE
    carregar_dados_lote.clear()
    carregar_lotes_do_projeto.clear()
    
    return True

def processar_upload_lotes(df, nome_arquivo):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_projetos = ss.worksheet("projetos")
    ws_lotes = ss.worksheet("controle_lotes")
    ws_dados = ss.worksheet("dados_brutos")
    
    # Converte tudo para string
    df = df.astype(str).replace("nan", "")
    
    # --- AJUSTE NOME LIMPO ---
    # Remove a extensão .xlsx ou .xls do nome
    nome_limpo = nome_arquivo.replace(".xlsx", "").replace(".xls", "")
    # -------------------------
    
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
            lista_dados.append([id_projeto, num_lote, str(ean).strip(), str(desc).strip(), ""])
            
        lista_lotes.append([id_projeto, num_lote, "Livre", "", f"0/{len(df_lote)}"])

    # Salva com o nome limpo (sem .xlsx)
    ws_projetos.append_row([id_projeto, nome_limpo, data_hoje, int(total_lotes), "Ativo"])
    ws_lotes.append_rows(lista_lotes)
    ws_dados.append_rows(lista_dados)
    
    return id_projeto, total_lotes

# --- 4. TELAS DE INTERFACE ---

def tela_login():
    if 'usuario_logado_temp' in st.session_state:
        return st.session_state['usuario_logado_temp']

    cookie_manager = get_manager()
    cookie_usuario = cookie_manager.get(cookie="usuario_coleta")
    
    if cookie_usuario:
        st.session_state['usuario_logado_temp'] = cookie_usuario
        return cookie_usuario

    st.title("🔒 Acesso Restrito - Coleta")
    
    try: usuarios = st.secrets["passwords"]
    except: st.error("Erro: Configure os Secrets [passwords]."); st.stop()

    col1, col2 = st.columns([2,1])
    with col1:
        user_input = st.selectbox("Usuário", ["Selecione..."] + list(usuarios.keys()))
        pass_input = st.text_input("Senha", type="password")
        
        if st.button("Entrar", type="primary"):
            if user_input != "Selecione..." and pass_input == usuarios[user_input]:
                st.session_state['usuario_logado_temp'] = user_input
                try:
                    cookie_manager.set("usuario_coleta", user_input, expires_at=datetime.now() + timedelta(days=1))
                except: pass
                
                st.rerun()
            else:
                st.error("Senha incorreta.")
    st.stop()

def remove_accents(input_str):
    """Remove acentos e caracteres especiais: Descrição -> descricao"""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def gerar_modelo_padrao():
    """Gera um arquivo Excel vazio apenas com os cabeçalhos corretos"""
    # Cria um DataFrame vazio com as colunas exatas
    df_modelo = pd.DataFrame(columns=["ean", "descricao"])
    
    # Gera o arquivo em memória
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_modelo.to_excel(writer, index=False)
    
    return output.getvalue()

def tela_admin_area():
    st.markdown("## ⚙️ Painel do Administrador")
    
    aba1, aba2 = st.tabs(["📤 Criar Novo Projeto", "📥 Baixar Relatórios"])
    
    with aba1:
        col_up, col_down = st.columns([3, 1])
        
        with col_down:
            st.markdown("### 1º Passo")
            st.markdown("Baixe a planilha modelo para preencher.")
            st.download_button(
                label="📥 Baixar Modelo (.xlsx)",
                data=gerar_modelo_padrao(),
                file_name="modelo_importacao_links.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        with col_up:
            st.markdown("### 2º Passo")
            st.markdown("Suba o modelo preenchido aqui.")
            arquivo = st.file_uploader("Arquivo Excel (Modelo Padrão)", type=["xlsx"])
            
            if arquivo:
                if st.button("🚀 Processar e Criar", type="primary"):
                    try:
                        df = pd.read_excel(arquivo, dtype=str)
                        df.columns = [str(c).lower().strip() for c in df.columns]
                        
                        if 'ean' in df.columns and 'descricao' in df.columns:
                            with st.spinner("Processando e enviando para o Google..."):
                                id_proj, qtd = processar_upload_lotes(df, arquivo.name)
                                st.success(f"Projeto criado com sucesso! ID: {id_proj}")
                                st.info(f"Total de Lotes gerados: {qtd}")
                                st.balloons()
                        else:
                            st.error("❌ O arquivo não segue o modelo padrão (colunas 'ean' e 'descricao').")
                    except Exception as e:
                        st.error(f"Erro ao ler arquivo: {e}")
    
    with aba2:
        st.write("Baixe o arquivo final com os links coletados.")
        projetos = carregar_projetos_ativos()
        if not projetos.empty:
            # --- AJUSTE VISUALIZAÇÃO ---
            # Mostra apenas o NOME, sem a data
            proj_dict = {row['nome']: row['id'] for _, row in projetos.iterrows()}
            # ---------------------------
            
            sel_proj = st.selectbox("Escolha o Projeto:", list(proj_dict.keys()))
            id_sel = proj_dict[sel_proj]
            
            if st.button("📦 Preparar Download"):
                with st.spinner("Baixando dados do Google e gerando Excel..."):
                    excel_data = baixar_projeto_completo(id_sel)
                    st.download_button(
                        label="📥 Clique para Baixar (.xlsx)",
                        data=excel_data,
                        file_name=f"Resultado_{sel_proj}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        else:
            st.warning("Sem projetos ativos.")

def tela_producao(usuario):
    st.title(f"🏭 Área de Coleta | {usuario}")
    
    # 1. Carrega projetos
    projetos = carregar_projetos_ativos()
    if projetos.empty:
        st.info("Nenhum projeto ativo no momento. Aguarde o Admin fazer upload.")
        return

    # Dropdown de Projetos
    proj_dict = {row['nome']: row['id'] for _, row in projetos.iterrows()}
    nome_proj = st.selectbox("Selecione o Projeto:", ["Selecione..."] + list(proj_dict.keys()))
    
    if nome_proj == "Selecione...": st.stop()
    id_proj = proj_dict[nome_proj]
    
    df_lotes = carregar_lotes_do_projeto(id_proj)
    if df_lotes.empty:
        st.warning("Projeto sem lotes gerados.")
        return

    # --- MAPA DE STATUS ---
    with st.expander("📊 Ver Mapa de Status (Todos os Lotes)", expanded=False):
        if not df_lotes.empty:
            df_view = df_lotes.copy()
            mapa_status = {"Livre": "Pendente", "Em Andamento": "Em andamento", "Concluído": "Concluída"}
            df_view['status'] = df_view['status'].map(mapa_status).fillna(df_view['status'])
            df_view['usuario'] = df_view.apply(lambda x: "-" if x['status'] == "Pendente" else x['usuario'], axis=1)
            df_view = df_view.sort_values(by='lote')
            
            df_final = df_view[['usuario', 'lote', 'status']]
            df_final.columns = ["Responsável", "Lote", "Status"]
            
            st.dataframe(df_final, hide_index=True, use_container_width=True)
        else:
            st.write("Sem dados.")

    st.divider()

    # --- GERENCIAR TRABALHO ---
    meus_lotes_ids = df_lotes[(df_lotes['status'] == 'Em Andamento') & (df_lotes['usuario'] == usuario)]['lote'].unique()
    lotes_livres_ids = df_lotes[df_lotes['status'] == 'Livre']['lote'].unique()
    
    opcoes_dropdown = []
    for l in sorted(meus_lotes_ids): opcoes_dropdown.append(f"Lote {l} (RETOMAR SEU TRABALHO)")
    for l in sorted(lotes_livres_ids): opcoes_dropdown.append(f"Lote {l} (PEGAR NOVO)")
        
    st.markdown("### 🚀 Gerenciar Trabalho")
    
    if not opcoes_dropdown: 
        st.info("Não há lotes disponíveis para você.")
    else:
        col_sel_1, col_sel_2 = st.columns([3, 1])
        with col_sel_1:
            escolha = st.selectbox("Escolha um lote:", ["Selecione..."] + opcoes_dropdown, label_visibility="collapsed")
        
        with col_sel_2:
            # --- PROTEÇÃO 1: BOTÃO DE ACESSAR LOTE ---
            # Placeholder segura o lugar do botão
            ph_btn_acessar = st.empty()
            
            if escolha != "Selecione...":
                # Se clicar, o botão some e vira mensagem de carregamento
                if ph_btn_acessar.button("Acessar Lote", type="primary", use_container_width=True):
                    ph_btn_acessar.info("⏳ Reservando...") # Bloqueio visual imediato
                    
                    num_lote_selecionado = int(escolha.split()[1])
                    
                    if num_lote_selecionado in lotes_livres_ids:
                        if reservar_lote(id_proj, num_lote_selecionado, usuario):
                            st.session_state['lote_trabalho'] = num_lote_selecionado
                            st.success(f"Lote {num_lote_selecionado} reservado!")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Erro: Lote já pego.")
                            time.sleep(2)
                            st.rerun()
                    else:
                        st.session_state['lote_trabalho'] = num_lote_selecionado
                        st.rerun()

    # --- EDITOR ---
    if 'lote_trabalho' in st.session_state:
        st.divider()
        num_lote = st.session_state['lote_trabalho']
        st.markdown(f"## 📝 Editando **Lote {num_lote}**")
        
        df_dados = carregar_dados_lote(id_proj, num_lote)
        
        # Auto-Save
        if "editor_links" in st.session_state:
            changes = st.session_state["editor_links"].get("edited_rows", {})
            if changes:
                for idx, val in changes.items():
                    if "link" in val:
                        novo_valor = val["link"]
                        sucesso = salvar_alteracao_individual(id_proj, num_lote, idx, novo_valor, df_dados)
                        if sucesso:
                            st.toast(f"Salvo!", icon="☁️")
                            df_dados.at[idx, 'link'] = novo_valor

        edited_df = st.data_editor(
            df_dados,
            key="editor_links",
            column_config={
                "id_projeto": None, "lote": None,
                "ean": st.column_config.TextColumn("EAN", disabled=True),
                "descricao": st.column_config.TextColumn("Descrição", disabled=True, width="medium"),
                "link": st.column_config.LinkColumn(
                    "Link", validate="^https?://", width="large",
                    help="Cole o link aqui. Se não encontrar, DEIXE EM BRANCO."
                )
            },
            hide_index=True, use_container_width=True, num_rows="fixed", height=600
        )
        
        # Progresso
        total = len(edited_df)
        preenchidos = edited_df['link'].replace('', pd.NA).count()
        vazios = total - preenchidos
        if total > 0:
            pct = int((preenchidos / total) * 100)
            st.progress(pct, text=f"Progresso: {preenchidos} preenchidos | {vazios} em branco")
        
        c1, c2 = st.columns(2)
        
        # --- PROTEÇÃO 2: BOTÃO SALVAR ---
        with c1:
            ph_btn_salvar = st.empty()
            if ph_btn_salvar.button("💾 Forçar Salvamento"):
                # Remove botão instantaneamente
                ph_btn_salvar.warning("⏳ Salvando dados...")
                with st.spinner("Enviando para o Google..."):
                    salvar_progresso_lote(edited_df, id_proj, num_lote, False)
                    st.toast("Salvo com sucesso!", icon="✅")
                    time.sleep(1) # Delay visual pequeno
                # Botão reaparece no rerun
                st.rerun()
        
        # --- PROTEÇÃO 3: BOTÃO ENTREGAR ---
        with c2:
            ph_btn_entregar = st.empty()
            # Botão verde (primary)
            if ph_btn_entregar.button("✅ Entregar Lote (Finalizar)", type="primary"):
                # Remove botão instantaneamente para evitar duplo clique
                ph_btn_entregar.warning("🚀 Enviando e finalizando... Por favor aguarde!")
                
                if vazios > 0:
                    st.toast(f"Entregando com {vazios} itens vazios.", icon="ℹ️")
                
                with st.spinner("Finalizando lote no sistema..."):
                    salvar_progresso_lote(edited_df, id_proj, num_lote, True)
                    del st.session_state['lote_trabalho']
                    st.balloons()
                    time.sleep(2)
                    st.rerun()
# --- MAIN COM ROTEAMENTO ---
def main():
    usuario_logado = tela_login()
    
    with st.sidebar:
        st.write(f"👤 **{usuario_logado}**")
        
        # --- ATUALIZAÇÃO: BOTÃO DE REFRESH ---
        if st.button("🔄 Atualizar Dados", help="Clique para baixar novos projetos ou lotes do Google"):
            st.cache_data.clear()
            st.toast("Dados atualizados com sucesso!", icon="✅")
            time.sleep(0.5)
            st.rerun()
        # --------------------------------------

        st.divider()
        
        # Botão de Sair
        if st.button("Sair"):
            get_manager().delete("usuario_coleta")
            if 'usuario_logado_temp' in st.session_state:
                del st.session_state['usuario_logado_temp']
            st.toast("Desconectando...", icon="👋")
            time.sleep(0.5) 
            st.rerun()
        
        st.divider()

    # Roteamento de Tela
    if usuario_logado in ADMINS:
        modo = st.sidebar.radio("Menu Admin", ["Produção", "Painel Admin"])
        if modo == "Painel Admin":
            tela_admin_area()
        else:
            tela_producao(usuario_logado)
    else:
        # Estagiário cai direto aqui
        tela_producao(usuario_logado)

if __name__ == "__main__":
    main()
