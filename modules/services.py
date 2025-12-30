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

TZ_BRASIL = timezone(timedelta(hours=-3))

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_manager():
    return stx.CookieManager()

@st.cache_resource
def get_client_google():
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Converte o objeto Secrets do Streamlit para um dicionário Python normal
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        
        # --- CORREÇÃO CRÍTICA PARA STREAMLIT CLOUD ---
        # Corrige a formatação da chave privada se o \n vier como texto literal
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        # ---------------------------------------------
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return gspread.authorize(creds)
        
    except Exception as e:
        st.error(f"Erro de Autenticação Google: {e}")
        return None

@st.cache_data(ttl=60)
def carregar_projetos_ativos():
    try:
        client = get_client_google()
        if not client: return pd.DataFrame() # Proteção extra
        
        ws = client.open("Sistema_Coleta_Links").worksheet("projetos")
        df = pd.DataFrame(ws.get_all_records())
        return df[df['status'] == 'Ativo'] if not df.empty else df
    except: return pd.DataFrame()

@st.cache_data(ttl=30)
def carregar_lotes_do_projeto(id_projeto):
    try:
        client = get_client_google()
        if not client: return pd.DataFrame()

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
        if not client: return pd.DataFrame()

        ws = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
        data = ws.get_all_records()
        
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

def salvar_alteracao_individual(id_projeto, numero_lote, idx, novo_link, df_origem):
    try: 
        linha_excel = int(df_origem.iloc[idx]['_row_index'])
    except: return False
        
    for i in range(3):
        try:
            client = get_client_google()
            ws = client.open("Sistema_Coleta_Links").worksheet("dados_brutos")
            ws.update_cell(linha_excel, 8, novo_link)
            carregar_dados_lote.clear()
            return True
        except Exception as e: 
            time.sleep(1)
    return False

def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False, checkpoint_val=""):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links")
    ws_d = ss.worksheet("dados_brutos")
    ws_l = ss.worksheet("controle_lotes")
    
    updates = []
    
    if '_row_index' in df_editado.columns:
        for _, row in df_editado.iterrows():
            linha = row['_row_index']
            updates.append({'range': f'H{linha}', 'values': [[row['link']]]})
    else:
        # Fallback
        todos = ws_d.get_all_records()
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
    
    if updates: ws_d.batch_update(updates)
    
    preenchidos = len(df_editado) - df_editado['link'].replace('', pd.NA).isna().sum()
    prog_str = f"{preenchidos}/{len(df_editado)}"
    
    lotes = ws_l.get_all_records()
    for i, row in enumerate(lotes):
        if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
            linha = i + 2
            ws_l.update_cell(linha, 5, prog_str)
            if checkpoint_val: ws_l.update_cell(linha, 6, checkpoint_val)
            if concluir:
                ws_l.update_cell(linha, 3, "Concluído")
                ws_l.update_cell(linha, 6, "")
            break
    
    carregar_dados_lote.clear()
    carregar_lotes_do_projeto.clear()
    return True

def salvar_log_tempo(usuario, id_proj, nome_proj, num_lote, duracao, acao, total, feitos):
    if duracao < 5: return 
    try:
        client = get_client_google()
        ss = client.open("Sistema_Coleta_Links")
        try: ws = ss.worksheet("registro_tempo")
        except: 
            ws = ss.add_worksheet("registro_tempo", 1000, 9)
            ws.append_row(["id", "lote", "data", "responsavel", "h_ini", "h_fim", "duracao", "projeto", "desc"])
        
        fim = datetime.now(TZ_BRASIL)
        ini = fim - timedelta(seconds=duracao)
        ws.append_row([str(uuid.uuid4()), str(num_lote), ini.strftime("%Y-%m-%d"), str(usuario), ini.strftime("%H:%M:%S"), fim.strftime("%H:%M:%S"), int(duracao), str(nome_proj), f"{acao} ({feitos}/{total})"])
    except: pass

def processar_upload(df, nome_arq):
    # AQUI PODEMOS TER ERRO SE O CLIENT FOR NONE
    client = get_client_google()
    if client is None:
        raise Exception("Falha na autenticação com o Google. Verifique os Logs.")

    ss = client.open("Sistema_Coleta_Links")
    
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
    df = pd.DataFrame(get_client_google().open("Sistema_Coleta_Links").worksheet("dados_brutos").get_all_records())
    if not df.empty:
        df = df.iloc[:, :8]; df.columns = ["id", "lote", "ean", "desc", "site", "cep", "end", "link"]
        df = df[df['id'] == str(id_p)][['ean', 'desc', 'link']]
        df.columns = ['EAN', 'Descrição', 'Link Coletado']
        
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
    return out.getvalue()

def gerar_modelo_padrao():
    df = pd.DataFrame(columns=["Site*", "Descrição*", "EAN*", "Quantidade no Lote*", "CEP", "Endereço"])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
    return out.getvalue()