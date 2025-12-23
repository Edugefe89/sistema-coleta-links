import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import uuid
import time
import io
import unicodedata
import extra_streamlit_components as stx

TZ_BRASIL = timezone(timedelta(hours=-3))

# --- FUNÇÕES UTILITÁRIAS ---
def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_manager():
    return stx.CookieManager()

# --- CONEXÃO GOOGLE ---
@st.cache_resource
def get_client_google():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        # Ajuste aqui conforme seus secrets
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro de Conexão Google: {e}")
        return None

# --- LEITURA DE DADOS ---
@st.cache_data(ttl=60)
def carregar_projetos_ativos():
    try:
        client = get_client_google()
        ws = client.open("Sistema_Coleta_Links").worksheet("projetos")
        df = pd.DataFrame(ws.get_all_records())
        if not df.empty:
            return df[df['status'] == 'Ativo']
        return df
    except: return pd.DataFrame()

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
            if len(df.columns) >= len(colunas_esperadas):
                df = df.iloc[:, :8]
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

# --- ESCRITA E LÓGICA ---
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
    try:
        ean_alvo = str(df_origem.iloc[indice_linha_df]['ean'])
    except: return False
    
    for tentativa in range(3):
        try:
            client = get_client_google()
            ws_dados = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
            cell = ws_dados.find(ean_alvo, in_column=3) 
            if cell:
                ws_dados.update_cell(cell.row, 8, novo_link)
                carregar_dados_lote.clear() # Limpa cache para consistência futura
                return True
            else: return False
        except Exception as e:
            time.sleep(2 ** (tentativa + 1))
    return False

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
            if checkpoint_val: ws_lotes.update_cell(linha_lote, 6, checkpoint_val)
            if concluir:
                ws_lotes.update_cell(linha_lote, 3, "Concluído")
                ws_lotes.update_cell(linha_lote, 6, "")
            break
    
    carregar_dados_lote.clear()
    carregar_lotes_do_projeto.clear()
    return True

def salvar_log_tempo(usuario, id_projeto, nome_projeto, numero_lote, duracao_segundos, acao, total_items, itens_feitos):
    if duracao_segundos < 5: return 
    client = get_client_google()
    try:
        ss = client.open("Sistema_Coleta_Links")
        try: ws = ss.worksheet("registro_tempo")
        except:
            ws = ss.add_worksheet("registro_tempo", rows=1000, cols=9)
            ws.append_row(["id", "lote", "data", "responsavel", "hora_inicio", "hora_fim", "duracao", "projeto", "descricao"])
        
        fim_dt = datetime.now(TZ_BRASIL)
        inicio_dt = fim_dt - timedelta(seconds=duracao_segundos)
        
        ws.append_row([
            str(uuid.uuid4()), str(numero_lote), inicio_dt.strftime("%Y-%m-%d"), str(usuario),
            inicio_dt.strftime("%H:%M:%S"), fim_dt.strftime("%H:%M:%S"), int(duracao_segundos),
            str(nome_projeto), f"{acao} - Progresso: {itens_feitos}/{total_items}"
        ])
    except: pass

def processar_upload_lotes(df, nome_arquivo):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    
    df = df.astype(str).replace("nan", "")
    colunas_contexto = [c for c in df.columns if any(x in c for x in ['site', 'cep', 'endereco'])]
    if colunas_contexto:
        df[colunas_contexto] = df[colunas_contexto].replace("", pd.NA).ffill().fillna("")

    id_projeto = str(uuid.uuid4())[:8]
    data_hoje = datetime.now(TZ_BRASIL).strftime("%d/%m/%Y")
    
    tamanho_lote = 100 
    if 'quantidadenolote*' in df.columns:
        try:
            val_raw = df.iloc[0]['quantidadenolote*']
            tamanho_lote = int(float(val_raw))
            if tamanho_lote <= 0: tamanho_lote = 100
        except: tamanho_lote = 100

    total_linhas = len(df)
    total_lotes = (total_linhas // tamanho_lote) + (1 if total_linhas % tamanho_lote > 0 else 0)
    
    lista_dados = []
    lista_lotes = []
    
    for i in range(total_lotes):
        num_lote = i + 1
        inicio = i * tamanho_lote
        fim = (i + 1) * tamanho_lote
        df_lote = df.iloc[inicio:fim]
        for _, row in df_lote.iterrows():
            lista_dados.append([
                id_projeto, num_lote, 
                str(row.get('ean*', '')).strip(), str(row.get('descricao*', '')).strip(), 
                str(row.get('site*', '')).strip(), str(row.get('cep', '')).strip(), str(row.get('endereco', '')).strip(), ""
            ])
        lista_lotes.append([id_projeto, num_lote, "Livre", "", f"0/{len(df_lote)}", ""])

    ss.worksheet("projetos").append_row([id_projeto, nome_arquivo.replace(".xlsx", ""), data_hoje, int(total_lotes), "Ativo"])
    ss.worksheet("controle_lotes").append_rows(lista_lotes)
    ss.worksheet("dados_brutos").append_rows(lista_dados)
    
    return id_projeto, total_lotes, tamanho_lote

def gerar_modelo_padrao():
    df_modelo = pd.DataFrame(columns=["Site*", "Descrição*", "EAN*", "Quantidade no Lote*", "CEP", "Endereço"])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_modelo.to_excel(writer, index=False)
    return output.getvalue()

def baixar_projeto_completo(id_projeto):
    client = get_client_google()
    df = pd.DataFrame(client.open("Sistema_Coleta_Links").worksheet("dados_brutos").get_all_records())
    
    colunas_esperadas = ["id_projeto", "lote", "ean", "descricao", "site", "cep", "endereco", "link"]
    if len(df.columns) >= len(colunas_esperadas):
        df = df.iloc[:, :8]
        df.columns = colunas_esperadas
    
    df_final = df[df['id_projeto'].astype(str) == str(id_projeto)].copy()
    if not df_final.empty:
        df_final = df_final[['ean', 'descricao', 'link']]
        df_final.columns = ['EAN', 'Descrição', 'Link Coletado']
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False, sheet_name='Links Coletados')
    return output.getvalue()