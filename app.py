import streamlit as st
import pandas as pd
import math
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import uuid
import time
import extra_streamlit_components as stx
import io
import unicodedata

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="Sistema Coleta Links", layout="wide", page_icon="🔗")

# --- DEFINIÇÃO DE FUSO HORÁRIO (BRASILIA -3) ---
TZ_BRASIL = timezone(timedelta(hours=-3))

# --- DEFINA AQUI QUEM SÃO OS ADMINS ---
ADMINS = ["admin", "Diego", "Eduardo"] 

# --- FUNÇÕES UTILITÁRIAS ---
def remove_accents(input_str):
    """Remove acentos e caracteres especiais: Descrição -> descricao"""
    if not isinstance(input_str, str): return str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def gerar_modelo_padrao():
    """Gera um arquivo Excel vazio com os novos cabeçalhos"""
    df_modelo = pd.DataFrame(columns=["Site", "Descrição", "EAN", "CEP", "Endereço"])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_modelo.to_excel(writer, index=False)
    return output.getvalue()

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
            colunas_esperadas = ["id_projeto", "lote", "ean", "descricao", "site", "cep", "endereco", "link"]
            if len(df.columns) == len(colunas_esperadas):
                df.columns = colunas_esperadas
            
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
    client = get_client_google()
    ws = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
    dados = ws.get_all_records()
    df = pd.DataFrame(dados)
    
    colunas_esperadas = ["id_projeto", "lote", "ean", "descricao", "site", "cep", "endereco", "link"]
    if len(df.columns) == len(colunas_esperadas):
        df.columns = colunas_esperadas

    df_final = df[df['id_projeto'].astype(str) == str(id_projeto)].copy()
    colunas_remover = ['id_projeto', 'lote']
    df_final = df_final.drop(columns=[c for c in colunas_remover if c in df_final.columns])
    
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
    """Salva um único link com retentativa (backoff)"""
    try:
        ean_alvo = str(df_origem.iloc[indice_linha_df]['ean'])
    except: return False
    
    max_tentativas = 3
    for tentativa in range(max_tentativas):
        try:
            client = get_client_google()
            ws_dados = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
            cell = ws_dados.find(ean_alvo, in_column=3) 
            
            if cell:
                ws_dados.update_cell(cell.row, 8, novo_link)
                carregar_dados_lote.clear()
                return True
            else: return False
                
        except Exception as e:
            erro_str = str(e).lower()
            if "quota" in erro_str or "429" in erro_str or "limit" in erro_str:
                time.sleep(2 ** (tentativa + 1)) 
            else:
                st.error(f"Erro ao salvar: {e}")
                return False
    return False

# --- ATUALIZADO: Salvar Progresso + Checkpoint ---
def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False, checkpoint_val=""):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_dados = ss.worksheet("dados_brutos")
    ws_lotes = ss.worksheet("controle_lotes")
    
    todos_dados = ws_dados.get_all_records()
    batch_updates = []
    mapa_linhas = {}
    
    for i, row in enumerate(todos_dados):
        val_id = str(row['id_projeto']) if 'id_projeto' in row else list(row.values())[0]
        val_lote = str(row['lote']) if 'lote' in row else list(row.values())[1]
        val_ean = str(row['ean']) if 'ean' in row else list(row.values())[2]

        if str(val_id) == str(id_projeto) and str(val_lote) == str(numero_lote):
            mapa_linhas[str(val_ean)] = i + 2
            
    for index, row in df_editado.iterrows():
        linha_sheet = mapa_linhas.get(str(row['ean']))
        if linha_sheet:
            novo_link = row['link']
            batch_updates.append({'range': f'H{linha_sheet}', 'values': [[novo_link]]})
            
    if batch_updates:
        ws_dados.batch_update(batch_updates)
        
    total_links = df_editado['link'].replace('', pd.NA).isna().sum()
    total_preenchidos = len(df_editado) - total_links
    progresso_str = f"{total_preenchidos}/{len(df_editado)}"
    
    todos_lotes = ws_lotes.get_all_records()
    for i, row in enumerate(todos_lotes):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            linha_lote = i + 2
            ws_lotes.update_cell(linha_lote, 5, progresso_str)
            
            if checkpoint_val:
                ws_lotes.update_cell(linha_lote, 6, checkpoint_val)
                
            if concluir:
                ws_lotes.update_cell(linha_lote, 3, "Concluído")
                ws_lotes.update_cell(linha_lote, 6, "") # Limpa checkpoint
            break
    
    carregar_dados_lote.clear()
    carregar_lotes_do_projeto.clear()
    return True

# --- REGISTRO DE TEMPO ---
def salvar_log_tempo(usuario, id_projeto, nome_projeto, numero_lote, duracao_segundos, acao, total_items, itens_feitos):
    if duracao_segundos < 5: return 

    client = get_client_google()
    try:
        ss = client.open("Sistema_Coleta_Links")
        try:
            ws = ss.worksheet("registro_tempo")
        except:
            ws = ss.add_worksheet("registro_tempo", rows=1000, cols=9)
            ws.append_row(["id", "lote", "data", "responsavel", "hora_inicio", "hora_fim", "duracao", "projeto", "descricao"])
        
        fim_dt = datetime.now(TZ_BRASIL)
        inicio_dt = fim_dt - timedelta(seconds=duracao_segundos)
        
        data_str = inicio_dt.strftime("%Y-%m-%d")
        hora_inicio_str = inicio_dt.strftime("%H:%M:%S")
        hora_fim_str = fim_dt.strftime("%H:%M:%S")
        
        tipo_acao = "Finalização de Lote" if acao == "Finalizar" else "Pausa/Salvamento"
        descricao_completa = f"{tipo_acao} - Progresso: {itens_feitos}/{total_items}"

        nova_linha = [
            str(uuid.uuid4()),      # id
            str(numero_lote),       # lote
            data_str,               # data
            str(usuario),           # responsavel
            hora_inicio_str,        # hora_inicio
            hora_fim_str,           # hora_fim
            int(duracao_segundos),  # duracao
            str(nome_projeto),      # projeto
            descricao_completa      # descricao
        ]
        ws.append_row(nova_linha)
    except Exception as e:
        print(f"Erro ao salvar tempo: {e}") 

def processar_upload_lotes(df, nome_arquivo):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_projetos = ss.worksheet("projetos")
    ws_lotes = ss.worksheet("controle_lotes")
    ws_dados = ss.worksheet("dados_brutos")
    
    df = df.astype(str).replace("nan", "")
    nome_limpo = nome_arquivo.replace(".xlsx", "").replace(".xls", "")
    
    id_projeto = str(uuid.uuid4())[:8]
    data_hoje = datetime.now(TZ_BRASIL).strftime("%d/%m/%Y")
    total_linhas = len(df)
    total_lotes = (total_linhas // 100) + (1 if total_linhas % 100 > 0 else 0)
    
    lista_dados = []
    lista_lotes = []
    
    for i in range(total_lotes):
        num_lote = i + 1
        inicio, fim = i * 100, (i + 1) * 100
        df_lote = df.iloc[inicio:fim]
        
        for _, row in df_lote.iterrows():
            ean = row.get('ean', '')
            desc = row.get('descricao', '')
            site = row.get('site', '')
            cep = row.get('cep', '')
            end = row.get('endereco', '')
            
            lista_dados.append([
                id_projeto, num_lote, 
                str(ean).strip(), str(desc).strip(), 
                str(site).strip(), str(cep).strip(), str(end).strip(), 
                ""
            ])
            
        lista_lotes.append([id_projeto, num_lote, "Livre", "", f"0/{len(df_lote)}", ""])

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
                try: cookie_manager.set("usuario_coleta", user_input, expires_at=datetime.now(TZ_BRASIL) + timedelta(days=1))
                except: pass
                st.rerun()
            else: st.error("Senha incorreta.")
    st.stop()

def tela_admin_area():
    st.markdown("## ⚙️ Painel do Administrador")
    aba1, aba2 = st.tabs(["📤 Criar Novo Projeto", "📥 Baixar Relatórios"])
    
    with aba1:
        col_up, col_down = st.columns([3, 1])
        with col_down:
            st.markdown("### 1º Passo")
            st.markdown("Baixe a planilha modelo atualizada.")
            st.download_button("📥 Baixar Modelo (.xlsx)", gerar_modelo_padrao(), "modelo_importacao.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            
        with col_up:
            st.markdown("### 2º Passo")
            st.markdown("Suba o modelo preenchido.")
            arquivo = st.file_uploader("Arquivo Excel", type=["xlsx"])
            
            if arquivo:
                if st.button("🚀 Processar e Criar", type="primary"):
                    try:
                        df = pd.read_excel(arquivo, dtype=str)
                        df.columns = [remove_accents(str(c).lower().strip().replace(" ","")) for c in df.columns]
                        
                        if 'ean' in df.columns and 'descricao' in df.columns:
                            with st.spinner("Enviando para o Google..."):
                                id_proj, qtd = processar_upload_lotes(df, arquivo.name)
                                st.success(f"Criado! ID: {id_proj}")
                                st.info(f"Lotes: {qtd}")
                                st.balloons()
                        else:
                            st.error("Erro: Colunas obrigatórias 'ean' e 'descricao' não encontradas.")
                    except Exception as e: st.error(f"Erro: {e}")
    
    with aba2:
        projetos = carregar_projetos_ativos()
        if not projetos.empty:
            proj_dict = {row['nome']: row['id'] for _, row in projetos.iterrows()}
            sel_proj = st.selectbox("Escolha o Projeto:", list(proj_dict.keys()))
            id_sel = proj_dict[sel_proj]
            
            if st.button("📦 Preparar Download"):
                with st.spinner("Baixando..."):
                    excel_data = baixar_projeto_completo(id_sel)
                    st.download_button("📥 Baixar (.xlsx)", excel_data, f"Resultado_{sel_proj}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else: st.warning("Sem projetos ativos.")

def tela_producao(usuario):
    st.title(f"🏭 Área de Coleta | {usuario}")
    
    projetos = carregar_projetos_ativos()
    if projetos.empty:
        st.info("Aguarde o Admin fazer upload.")
        return

    proj_dict = {row['nome']: row['id'] for _, row in projetos.iterrows()}
    nome_proj = st.selectbox("Selecione o Projeto:", ["Selecione..."] + list(proj_dict.keys()))
    
    if nome_proj == "Selecione...": st.stop()
    id_proj = proj_dict[nome_proj]
    
    # Recarrega lotes para garantir que checkpoints novos apareçam
    st.cache_data.clear() 
    df_lotes = carregar_lotes_do_projeto(id_proj)
    
    if df_lotes.empty:
        st.warning("Sem lotes gerados.")
        return

    # --- VISUALIZAÇÃO GERAL ---
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
        else: st.write("Sem dados.")

    st.divider()

    # --- SELEÇÃO DE LOTE ---
    if 'lote_trabalho' not in st.session_state:
        meus_lotes_ids = df_lotes[(df_lotes['status'] == 'Em Andamento') & (df_lotes['usuario'] == usuario)]['lote'].unique()
        lotes_livres_ids = df_lotes[df_lotes['status'] == 'Livre']['lote'].unique()
        
        opcoes_dropdown = []
        for l in sorted(meus_lotes_ids): opcoes_dropdown.append(f"Lote {l} (RETOMAR SEU TRABALHO)")
        for l in sorted(lotes_livres_ids): opcoes_dropdown.append(f"Lote {l} (PEGAR NOVO)")
            
        st.markdown("### 🚀 Gerenciar Trabalho")
        
        if not opcoes_dropdown: 
            st.info("Não há lotes disponíveis.")
        else:
            col_sel_1, col_sel_2 = st.columns([3, 1])
            with col_sel_1:
                escolha = st.selectbox("Escolha:", ["Selecione..."] + opcoes_dropdown, label_visibility="collapsed")
            
            with col_sel_2:
                ph_btn_acessar = st.empty()
                if escolha != "Selecione...":
                    if ph_btn_acessar.button("Acessar Lote", type="primary", use_container_width=True):
                        ph_btn_acessar.info("⏳ Reservando...") 
                        num_lote_selecionado = int(escolha.split()[1])
                        
                        pode_entrar = False
                        if num_lote_selecionado in lotes_livres_ids:
                            if reservar_lote(id_proj, num_lote_selecionado, usuario):
                                pode_entrar = True
                            else:
                                st.error("Erro: Lote já pego.")
                                time.sleep(2); st.rerun()
                        else:
                            pode_entrar = True
                        
                        if pode_entrar:
                            st.session_state['lote_trabalho'] = num_lote_selecionado
                            st.session_state['status_trabalho'] = 'TRABALHANDO' 
                            st.session_state['hora_inicio_sessao'] = datetime.now(TZ_BRASIL) 
                            st.success("Reservado!")
                            time.sleep(0.5); st.rerun()

    # --- ÁREA DE TRABALHO (DENTRO DO LOTE) ---
    else:
        num_lote = st.session_state['lote_trabalho']
        df_dados = carregar_dados_lote(id_proj, num_lote)
        
        # --- LÓGICA DO CHECKPOINT VISUAL (BLINDADA) ---
        lote_info = df_lotes[df_lotes['lote'] == str(num_lote)]
        checkpoint_salvo = ""
        
        # Verifica se a coluna existe e se tem dados
        if not lote_info.empty and 'checkpoint' in lote_info.columns:
            val = lote_info.iloc[0]['checkpoint']
            # Converte para string e remove espaços vazios para garantir
            checkpoint_salvo = str(val).strip() if val else ""

        # Função de comparação segura
        def verificar_marcador(descricao_linha):
            desc_str = str(descricao_linha).strip()
            # Só marca se o checkpoint não for vazio/nan e for igual a linha
            if checkpoint_salvo and checkpoint_salvo != "nan" and checkpoint_salvo == desc_str:
                return ">>> PAREI AQUI <<<"
            return ""

        # Aplica o marcador
        df_dados.insert(0, "MARCADOR", df_dados['descricao'].apply(verificar_marcador))

        modo_atual = st.session_state.get('status_trabalho', 'TRABALHANDO')

        # ---------------------------------------------------------
        # TELA 1: MODO PAUSA
        # ---------------------------------------------------------
        if modo_atual == 'PAUSADO':
            st.warning(f"⏸️ **Lote {num_lote} Pausado**")
            st.info("O tempo não está sendo contabilizado agora. Clique abaixo para continuar.")
            if st.button("▶️ RETOMAR TRABALHO", type="primary", use_container_width=True):
                st.session_state['status_trabalho'] = 'TRABALHANDO'
                st.session_state['hora_inicio_sessao'] = datetime.now(TZ_BRASIL) 
                st.rerun()

        # ---------------------------------------------------------
        # TELA 2: MODO TRABALHO
        # ---------------------------------------------------------
        else:
            if 'hora_inicio_sessao' not in st.session_state:
                st.session_state['hora_inicio_sessao'] = datetime.now(TZ_BRASIL)

            st.divider()
            
            # Mostra visualmente onde o sistema acha que parou (Debug para o usuário)
            if checkpoint_salvo and checkpoint_salvo != "nan":
                st.info(f"📍 **Retomando do ponto:** {checkpoint_salvo}")
                st.markdown(f"## 📝 Editando **Lote {num_lote}**")
            else:
                st.markdown(f"## 📝 Editando **Lote {num_lote}**")
            
            if "editor_links" in st.session_state:
                changes = st.session_state["editor_links"].get("edited_rows", {})
                if changes:
                    for idx, val in changes.items():
                        if "link" in val:
                            novo_valor = val["link"]
                            if salvar_alteracao_individual(id_proj, num_lote, idx, novo_valor, df_dados):
                                st.toast("Salvo!", icon="☁️"); df_dados.at[idx, 'link'] = novo_valor

            edited_df = st.data_editor(
                df_dados,
                key="editor_links",
                column_config={
                    "id_projeto": None, "lote": None,
                    # Config da coluna Checkpoint
                    "MARCADOR": st.column_config.TextColumn("Marcador", width="medium", disabled=True),
                    
                    "ean": st.column_config.TextColumn("EAN", disabled=True),
                    "descricao": st.column_config.TextColumn("Descrição", disabled=True, width="medium"),
                    "site": st.column_config.LinkColumn("Site Referência", display_text="🔗 Acessar", disabled=True, width="small"),
                    "cep": st.column_config.TextColumn("CEP", disabled=True),
                    "endereco": st.column_config.TextColumn("Endereço", disabled=True),
                    "link": st.column_config.LinkColumn("Link Coletado (Cole Aqui)", validate="^https?://", width="large")
                },
                hide_index=True, use_container_width=True, num_rows="fixed", height=600
            )
            
            total = len(edited_df)
            preenchidos = edited_df['link'].replace('', pd.NA).count()
            vazios = total - preenchidos
            if total > 0:
                pct = int((preenchidos / total) * 100)
                st.progress(pct, text=f"Progresso: {preenchidos} preenchidos | {vazios} em branco")
            
            c1, c2 = st.columns(2)
            
            # --- ÁREA DE PAUSA ---
            with c1:
                st.markdown("### ⏸️ Pausar")
                lista_descricoes = df_dados['descricao'].tolist()
                
                # Tenta pré-selecionar a última posição salva se existir, senão pega a primeira
                index_default = 0
                if checkpoint_salvo in lista_descricoes:
                     index_default = lista_descricoes.index(checkpoint_salvo) + 1 # +1 pois o primeiro é "Não marcar"

                item_selecionado = st.selectbox(
                    "Onde você parou? (Isso criará um marcador visual na volta)", 
                    options=["Não marcar nada"] + lista_descricoes,
                    index=0 # Reseta para 0 para forçar o usuário a escolher, ou use index_default se preferir memória
                )
                
                ph_btn_salvar = st.empty()
                if ph_btn_salvar.button("💾 Salvar Checkpoint e Pausar"):
                    ph_btn_salvar.warning("⏳ Salvando e Pausando...")
                    
                    tempo_decorrido = 0
                    if 'hora_inicio_sessao' in st.session_state:
                        delta = datetime.now(TZ_BRASIL) - st.session_state['hora_inicio_sessao']
                        tempo_decorrido = delta.total_seconds()
                        salvar_log_tempo(usuario, id_proj, nome_proj, num_lote, tempo_decorrido, "Salvar_Pausa", total, preenchidos)
                    
                    valor_checkpoint = item_selecionado if item_selecionado != "Não marcar nada" else ""

                    with st.spinner("Enviando..."):
                        salvar_progresso_lote(edited_df, id_proj, num_lote, False, checkpoint_val=valor_checkpoint)
                    
                    st.session_state['status_trabalho'] = 'PAUSADO'
                    if 'hora_inicio_sessao' in st.session_state: del st.session_state['hora_inicio_sessao']
                    
                    st.toast(f"Pausado em: {valor_checkpoint}", icon="✅")
                    time.sleep(1); st.rerun()
            
            # BOTÃO FINALIZAR
            with c2:
                st.markdown("### ✅ Finalizar")
                st.write("Concluiu tudo?") 
                ph_btn_entregar = st.empty()
                if ph_btn_entregar.button("Entregar Lote Completo", type="primary"):
                    ph_btn_entregar.warning("🚀 Finalizando...")
                    tempo_decorrido = 0
                    if 'hora_inicio_sessao' in st.session_state:
                        delta = datetime.now(TZ_BRASIL) - st.session_state['hora_inicio_sessao']
                        tempo_decorrido = delta.total_seconds()
                        salvar_log_tempo(usuario, id_proj, nome_proj, num_lote, tempo_decorrido, "Finalizar", total, preenchidos)

                    if vazios > 0: st.toast(f"Entregando com {vazios} itens vazios.", icon="ℹ️")
                    with st.spinner("Processando..."):
                        salvar_progresso_lote(edited_df, id_proj, num_lote, True)
                        
                        keys_to_clear = ['lote_trabalho', 'hora_inicio_sessao', 'status_trabalho']
                        for k in keys_to_clear:
                            if k in st.session_state: del st.session_state[k]
                        
                        st.balloons(); time.sleep(2); st.rerun()

# --- MAIN ---
def main():
    usuario_logado = tela_login()
    
    with st.sidebar:
        st.write(f"👤 **{usuario_logado}**")
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            st.toast("Atualizado!", icon="✅"); time.sleep(0.5); st.rerun()
        st.divider()
        if st.button("Sair"):
            get_manager().delete("usuario_coleta")
            if 'usuario_logado_temp' in st.session_state: del st.session_state['usuario_logado_temp']
            st.rerun()
        st.divider()

    if usuario_logado in ADMINS:
        modo = st.sidebar.radio("Menu Admin", ["Produção", "Painel Admin"])
        if modo == "Painel Admin": tela_admin_area()
        else: tela_producao(usuario_logado)
    else:
        tela_producao(usuario_logado)

if __name__ == "__main__":
    main()
