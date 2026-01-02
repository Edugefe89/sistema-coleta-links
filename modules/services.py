import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import uuid
import time
import io
import unicodedata
import extra_streamlit_components as stx
import random

# --- CONFIGURAÇÃO ---
TZ_BRASIL = timezone(timedelta(hours=-3))

# 🔴 COLOQUE O ID DA SUA PLANILHA DE COLETA AQUI 🔴
ID_PLANILHA_COLETA = "COLE_O_ID_DA_SUA_PLANILHA_AQUI" 

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_manager():
    return stx.CookieManager()

# --- FUNÇÃO DE RETRY INTELIGENTE (Exponential Backoff) ---
def retry_api(func, *args, **kwargs):
    """
    Tenta executar uma função do Gspread. 
    Se der erro de cota (429), espera progressivamente (2s, 4s, 8s...)
    """
    max_tentativas = 4
    for i in range(max_tentativas):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Se for a última tentativa, explode o erro
            if i == max_tentativas - 1:
                print(f"❌ Erro fatal após {max_tentativas} tentativas: {e}")
                return False
            
            # Se for erro de cota (429) ou conexão, espera
            wait_time = (2 ** i) + random.uniform(0, 1) # Backoff Exponencial + Jitter
            print(f"⚠️ Cota cheia ou erro. Tentativa {i+1}/{max_tentativas}. Esperando {wait_time:.2f}s...")
            time.sleep(wait_time)
    return False

@st.cache_resource
def get_client_coleta():
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # --- MUDANÇA: Busca o segredo do NOVO robô ---
        creds_dict = dict(st.secrets["connections"]["gsheets_coleta"])
        
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return gspread.authorize(creds)
        
    except Exception as e:
        st.error(f"Erro de Autenticação (Robô Coleta): {e}")
        return None

def abrir_planilha(client):
    return retry_api(client.open_by_key, ID_PLANILHA_COLETA)

# --- FUNÇÕES DE LEITURA (CACHE ALTO PARA ECONOMIZAR) ---

@st.cache_data(ttl=60)
def carregar_projetos_ativos():
    try:
        client = get_client_coleta()
        if not client: return pd.DataFrame() 
        
        ss = abrir_planilha(client)
        if not ss: return pd.DataFrame()

        ws = ss.worksheet("projetos")
        data = retry_api(ws.get_all_records)
        if not data: return pd.DataFrame()

        df = pd.DataFrame(data)
        return df[df['status'] == 'Ativo'] if not df.empty else df
    except: return pd.DataFrame()

@st.cache_data(ttl=60)
def carregar_lotes_do_projeto(id_projeto):
    try:
        client = get_client_coleta()
        if not client: return pd.DataFrame()

        ss = abrir_planilha(client)
        ws = ss.worksheet("controle_lotes")
        data = retry_api(ws.get_all_records)
        if not data: return pd.DataFrame()

        df = pd.DataFrame(data)
        if not df.empty:
            df['id_projeto'] = df['id_projeto'].astype(str)
            return df[df['id_projeto'] == str(id_projeto)]
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=300) 
def carregar_dados_lote(id_projeto, numero_lote):
    try:
        client = get_client_coleta()
        if not client: return pd.DataFrame()

        ss = abrir_planilha(client)
        ws = ss.worksheet("dados_brutos")
        data = retry_api(ws.get_all_records)
        if not data: return pd.DataFrame()
        
        for i, row in enumerate(data):
            row['_row_index'] = i + 2
            
        df = pd.DataFrame(data)
        
        if not df.empty:
            cols_desejadas = ["id_projeto", "lote", "ean", "descricao", "site", "cep", "endereco", "link", "_row_index"]
            cols_existentes = [c for c in cols_desejadas if c in df.columns]
            df = df[cols_existentes]
            
            df['id_projeto'] = df['id_projeto'].astype(str)
            df['lote'] = df['lote'].astype(str)
            
            return df[(df['id_projeto'] == str(id_projeto)) & (df['lote'] == str(numero_lote))]
        return df
    except: return pd.DataFrame()

# --- FUNÇÕES DE ESCRITA (COM RETRY AUTOMÁTICO) ---

def reservar_lote(id_projeto, numero_lote, usuario):
    client = get_client_coleta()
    ss = abrir_planilha(client)
    if not ss: return False
    ws = ss.worksheet("controle_lotes")
    
    registros = retry_api(ws.get_all_records)
    if not registros: return False

    for i, row in enumerate(registros):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            linha = i + 2 
            if row['status'] == "Livre" or (row['status'] == "Em Andamento" and row['usuario'] == usuario):
                # Usa Retry na escrita
                return retry_api(
                    ws.update, 
                    range_name=f"C{linha}:D{linha}", 
                    values=[["Em Andamento", usuario]]
                )
    return False

def salvar_alteracao_individual(id_projeto, numero_lote, idx, novo_link, df_origem):
    try: 
        linha_excel = int(df_origem.iloc[idx]['_row_index'])
    except: return False
    
    client = get_client_coleta()
    ss = abrir_planilha(client)
    ws = ss.worksheet("dados_brutos")
    
    # Usa Retry na escrita
    res = retry_api(ws.update_cell, linha_excel, 8, novo_link)
    if res:
        carregar_dados_lote.clear()
        return True
    return False

def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False, checkpoint_val=""):
    client = get_client_coleta()
    ss = abrir_planilha(client)
    ws_d = ss.worksheet("dados_brutos")
    ws_l = ss.worksheet("controle_lotes")
    
    # 1. Salva os links (Batch Update)
    updates = []
    if '_row_index' in df_editado.columns:
        for _, row in df_editado.iterrows():
            linha = row['_row_index']
            updates.append({'range': f'H{linha}', 'values': [[row['link']]]})
    else:
        todos = retry_api(ws_d.get_all_records)
        if todos:
            mapa = {}
            for i, row in enumerate(todos):
                rid = str(row.get('id_projeto', list(row.values())[0]))
                rlote = str(row.get('lote', list(row.values())[1]))
                rean = str(row.get('ean', list(row.values())[2]))
                if rid == str(id_projeto) and rlote == str(numero_lote):
                    mapa[rean] = i + 2
            for _, row in df_editado.iterrows():
                linha = mapa.get(str(row['ean']))
                if linha: updates.append({'range': f'H{linha}', 'values': [[row['link']]]})
    
    if updates:
        retry_api(ws_d.batch_update, updates)
    
    # 2. Atualiza o Controle de Lotes
    preenchidos = len(df_editado) - df_editado['link'].replace('', pd.NA).isna().sum()
    prog_str = f"{preenchidos}/{len(df_editado)}"
    
    lotes = retry_api(ws_l.get_all_records)
    if lotes:
        for i, row in enumerate(lotes):
            if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
                linha = i + 2
                
                if concluir:
                    usr_atual = row.get('usuario', '')
                    retry_api(
                        ws_l.update,
                        range_name=f"C{linha}:F{linha}", 
                        values=[["Concluído", usr_atual, prog_str, ""]]
                    )
                else:
                    vals = [prog_str]
                    rg = f"E{linha}"
                    if checkpoint_val: 
                        vals.append(checkpoint_val) 
                        rg = f"E{linha}:F{linha}"
                    
                    retry_api(ws_l.update, range_name=rg, values=[vals])
                break
    
    carregar_dados_lote.clear()
    carregar_lotes_do_projeto.clear()
    return True

def salvar_log_tempo(usuario, id_proj, nome_proj, num_lote, duracao, acao, total, feitos):
    if duracao < 5: return 
    try:
        client = get_client_coleta()
        ss = abrir_planilha(client)
        try: ws = ss.worksheet("registro_tempo")
        except: 
            ws = ss.add_worksheet("registro_tempo", 1000, 9)
            ws.append_row(["id", "lote", "data", "responsavel", "h_ini", "h_fim", "duracao", "projeto", "desc"])
        
        fim = datetime.now(TZ_BRASIL)
        ini = fim - timedelta(seconds=duracao)
        
        # Log é menos crítico, tentamos uma vez só, mas com proteção
        try:
            ws.append_row([str(uuid.uuid4()), str(num_lote), ini.strftime("%Y-%m-%d"), str(usuario), ini.strftime("%H:%M:%S"), fim.strftime("%H:%M:%S"), int(duracao), str(nome_proj), f"{acao} ({feitos}/{total})"])
        except: pass
    except: pass

def processar_upload(df, nome_arq):
    client = get_client_coleta()
    if client is None: raise Exception("Falha na autenticação.")

    ss = abrir_planilha(client)
    
    df = df.astype(str)
    for termo in ["nan", "None", "NaT", "<NA>"]:
        df = df.replace(termo, "")
    
    cols_ctx = [c for c in df.columns if any(x in c for x in ['site', 'cep', 'endereco'])]
    if cols_ctx: df[cols_ctx] = df[cols_ctx].replace("", pd.NA).ffill().fillna("")
    
    id_p = str(uuid.uuid4())[:8]
    tam = 100
    if 'quantidadenolote*' in df.columns:
        try: tam = int(float(df.iloc[0]['quantidadenolote*']))
        except: tam = 100
    if tam <= 0: tam = 100
        
    total_lotes = (len(df) // tam) + (1 if len(df) % tam > 0 else 0)
    l_dados, l_lotes = [], []
    
    for i in range(total_lotes):
        num = i + 1
        sub = df.iloc[i*tam : (i+1)*tam]
        for _, r in sub.iterrows():
            l_dados.append([
                id_p, num, 
                str(r.get('ean*','')).strip(), 
                str(r.get('descricao*','')).strip(), 
                str(r.get('site*','')).strip(), 
                str(r.get('cep','')).strip(), 
                str(r.get('endereco','')).strip(), 
                ""
            ])
        l_lotes.append([id_p, num, "Livre", "", f"0/{len(sub)}", ""])
        
    ss.worksheet("projetos").append_row([id_p, nome_arq.replace(".xlsx",""), datetime.now(TZ_BRASIL).strftime("%d/%m/%Y"), int(total_lotes), "Ativo"])
    ss.worksheet("controle_lotes").append_rows(l_lotes)
    ss.worksheet("dados_brutos").append_rows(l_dados)
    return id_p, len(df), tam

def baixar_excel(id_p):
    try:
        client = get_client_coleta()
        ss = abrir_planilha(client)
        
        # Retry na leitura dos dados brutos
        data = retry_api(ss.worksheet("dados_brutos").get_all_records)
        if not data: return None

        df = pd.DataFrame(data)
        
        if not df.empty:
            df = df.iloc[:, :8]; df.columns = ["id", "lote", "ean", "desc", "site", "cep", "end", "link"]
            df = df[df['id'] == str(id_p)][['ean', 'desc', 'link']]
            df.columns = ['EAN', 'Descrição', 'Link Coletado']
        
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
        return out.getvalue()
    except Exception as e:
        print(f"Erro download: {e}")
        return None

def gerar_modelo_padrao():
    df = pd.DataFrame(columns=["Site*", "Descrição*", "EAN*", "Quantidade no Lote*", "CEP", "Endereço"])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
    return out.getvalue()