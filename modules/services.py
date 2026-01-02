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

# 🔴 ID DA PLANILHA (CONFIRA SE ESTÁ CORRETO) 🔴
ID_PLANILHA_COLETA = "1IwV0h5HrqBkl4owb3lVzPIl2lLxj9n3cfH15U_SISlQ" 

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def get_manager():
    return stx.CookieManager()

# --- RETRY API (ESSENCIAL JÁ QUE NÃO TEM CACHE) ---
def retry_api(func, *args, **kwargs):
    """
    Tenta executar função do Gspread.
    Se der erro de cota (429), espera e tenta de novo.
    """
    max_tentativas = 5
    for i in range(max_tentativas):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == max_tentativas - 1:
                print(f"❌ Erro fatal API: {e}")
                raise e 
            
            # Espera progressiva: 1.5s, 3s, 6s...
            wait_time = (1.5 ** i) + random.uniform(0, 1) 
            time.sleep(wait_time)
    return None

# --- AUTENTICAÇÃO (SEM CACHE) ---
def get_client_coleta():
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds_dict = dict(st.secrets["connections"]["gsheets_coleta"])
        
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return gspread.authorize(creds)
        
    except Exception as e:
        st.error(f"Erro Auth Coleta: {e}")
        return None

def abrir_planilha(client):
    return retry_api(client.open_by_key, ID_PLANILHA_COLETA)

# --- FUNÇÕES DE LEITURA (AGORA EM TEMPO REAL) ---

def carregar_projetos_ativos():
    try:
        client = get_client_coleta()
        if not client: return pd.DataFrame() 
        
        ss = abrir_planilha(client)
        ws = ss.worksheet("projetos")
        data = retry_api(ws.get_all_records)
        
        if not data: return pd.DataFrame()

        df = pd.DataFrame(data)
        return df[df['status'] == 'Ativo'] if not df.empty else df
    except: return pd.DataFrame()

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

def carregar_dados_lote(id_projeto, numero_lote):
    try:
        client = get_client_coleta()
        if not client: return pd.DataFrame()

        ss = abrir_planilha(client)
        ws = ss.worksheet("dados_brutos")
        
        # Usa get_all_values para evitar erro de colunas vazias
        raw_data = retry_api(ws.get_all_values)
        
        if not raw_data or len(raw_data) < 2: 
            return pd.DataFrame()
        
        headers = raw_data.pop(0) 
        df = pd.DataFrame(raw_data, columns=headers)
        
        # Padroniza colunas (Minúsculo e sem espaço)
        df.columns = [str(c).lower().strip() for c in df.columns]

        # Garante índice
        if '_row_index' not in df.columns:
            df['_row_index'] = range(2, len(df) + 2)
            
        # Garante colunas essenciais
        cols_essenciais = ["id_projeto", "lote", "ean", "descricao", "site", "cep", "endereco", "link", "_row_index"]
        for col in cols_essenciais:
            if col not in df.columns:
                df[col] = "" 
        
        if not df.empty:
            df = df[cols_essenciais]
            df['id_projeto'] = df['id_projeto'].astype(str)
            df['lote'] = df['lote'].astype(str)
            return df[(df['id_projeto'] == str(id_projeto)) & (df['lote'] == str(numero_lote))]
        return df
    except Exception as e:
        print(f"Erro carregar dados: {e}")
        return pd.DataFrame()

# --- FUNÇÕES DE ESCRITA ---

def processar_upload(df, nome_arq):
    print("--- 🕵️‍♂️ INICIO DEBUG UPLOAD ---")
    
    try:
        client = get_client_coleta()
        if client is None: 
            print("❌ Falha: Cliente Google retornou None")
            raise Exception("Falha Auth.")
        print("✅ Cliente Google Autenticado")

        ss = abrir_planilha(client)
        print(f"✅ Planilha Aberta: {ss.title}")
        
        # DEBUG 1: O que chegou do Excel?
        print(f"📊 DataFrame Original - Linhas: {len(df)}")
        print(f"📊 Colunas Encontradas: {list(df.columns)}")

        df = df.astype(str)
        for termo in ["nan", "None", "NaT", "<NA>"]:
            df = df.replace(termo, "")
        
        cols_ctx = [c for c in df.columns if any(x in c for x in ['site', 'cep', 'endereco'])]
        if cols_ctx: df[cols_ctx] = df[cols_ctx].replace("", pd.NA).ffill().fillna("")
        
        id_p = str(uuid.uuid4())[:8]
        tam = 100
        
        # DEBUG 2: Verificação da Coluna de Quantidade
        if 'quantidadenolote*' in df.columns:
            try: 
                tam = int(float(df.iloc[0]['quantidadenolote*']))
                print(f"✅ Tamanho do lote definido via Excel: {tam}")
            except: 
                tam = 100
                print("⚠️ Erro ao ler tamanho do lote, usando padrão 100")
        else:
            print("⚠️ Coluna 'quantidadenolote*' não encontrada. Usando padrão 100.")
            
        if tam <= 0: tam = 100
            
        total_lotes = (len(df) // tam) + (1 if len(df) % tam > 0 else 0)
        l_dados, l_lotes = [], []
        
        print("⚙️ Processando linhas...")
        for i in range(total_lotes):
            num = i + 1
            sub = df.iloc[i*tam : (i+1)*tam]
            for _, r in sub.iterrows():
                # DEBUG 3: Verificação dos Nomes das Colunas
                # Se o Excel não tiver o "*" no nome, isso aqui vai virar vazio ""
                ean = str(r.get('ean*','')).strip()
                desc = str(r.get('descricao*','')).strip()
                
                l_dados.append([
                    id_p, num, 
                    ean, 
                    desc, 
                    str(r.get('site*','')).strip(), 
                    str(r.get('cep','')).strip(), 
                    str(r.get('endereco','')).strip(), 
                    ""
                ])
            l_lotes.append([id_p, num, "Livre", "", f"0/{len(sub)}", ""])
            
        # DEBUG 4: O Veredito antes de enviar
        print(f"📦 Total Lotes Gerados: {len(l_lotes)}")
        print(f"📦 Total Linhas de Dados: {len(l_dados)}")
        
        if len(l_dados) > 0:
            print(f"🔎 Exemplo Linha 1: {l_dados[0]}")
            if l_dados[0][2] == "": # Índice 2 é o EAN
                print("⚠️ ALERTA: O EAN está vazio! Verifique se a coluna no Excel se chama 'ean*' (com asterisco)")
        else:
            print("❌ ERRO CRÍTICO: A lista de dados está vazia! Nada será enviado.")

        # Envio
        print("🚀 Enviando para ABA PROJETOS...")
        retry_api(ss.worksheet("projetos").append_row, [id_p, nome_arq.replace(".xlsx",""), datetime.now(TZ_BRASIL).strftime("%d/%m/%Y"), int(total_lotes), "Ativo"])
        
        print("🚀 Enviando para ABA CONTROLE_LOTES...")
        retry_api(ss.worksheet("controle_lotes").append_rows, l_lotes)
        
        print("🚀 Enviando para ABA DADOS_BRUTOS...")
        retry_api(ss.worksheet("dados_brutos").append_rows, l_dados)
        
        print("✅ UPLOAD CONCLUÍDO COM SUCESSO")
        print("--- FIM DEBUG ---")
        return id_p, len(df), tam

    except Exception as e:
        print(f"❌ ERRO NO PROCESSO DE UPLOAD: {e}")
        raise e
        
    # Retry em cada etapa de gravação
    retry_api(ss.worksheet("projetos").append_row, [id_p, nome_arq.replace(".xlsx",""), datetime.now(TZ_BRASIL).strftime("%d/%m/%Y"), int(total_lotes), "Ativo"])
    retry_api(ss.worksheet("controle_lotes").append_rows, l_lotes)
    retry_api(ss.worksheet("dados_brutos").append_rows, l_dados)
    
    return id_p, len(df), tam

def reservar_lote(id_projeto, numero_lote, usuario):
    client = get_client_coleta()
    try:
        ss = abrir_planilha(client)
        ws = ss.worksheet("controle_lotes")
        registros = retry_api(ws.get_all_records)
        
        if not registros: return False
        
        for i, row in enumerate(registros):
            if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
                linha = i + 2 
                if row['status'] == "Livre" or (row['status'] == "Em Andamento" and row['usuario'] == usuario):
                    retry_api(ws.update, range_name=f"C{linha}:D{linha}", values=[["Em Andamento", usuario]])
                    return True
    except: pass
    return False

def salvar_alteracao_individual(id_projeto, numero_lote, idx, novo_link, df_origem):
    try: 
        linha_excel = int(df_origem.iloc[idx]['_row_index'])
    except: return False
    
    client = get_client_coleta()
    ss = abrir_planilha(client)
    ws = ss.worksheet("dados_brutos")
    
    try:
        retry_api(ws.update_cell, linha_excel, 8, novo_link)
        return True
    except: return False

def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False, checkpoint_val=""):
    client = get_client_coleta()
    ss = abrir_planilha(client)
    ws_d = ss.worksheet("dados_brutos")
    ws_l = ss.worksheet("controle_lotes")
    
    updates = []
    if '_row_index' in df_editado.columns:
        for _, row in df_editado.iterrows():
            linha = row['_row_index']
            updates.append({'range': f'H{linha}', 'values': [[row['link']]]})
    else:
        # Fallback de busca se não tiver índice
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
    
    if updates: retry_api(ws_d.batch_update, updates)
    
    preenchidos = len(df_editado) - df_editado['link'].replace('', pd.NA).isna().sum()
    prog_str = f"{preenchidos}/{len(df_editado)}"
    
    lotes = retry_api(ws_l.get_all_records)
    if lotes:
        for i, row in enumerate(lotes):
            if str(row['id_projeto']) == str(id_projeto) and str(row['lote']) == str(numero_lote):
                linha = i + 2
                if concluir:
                    usr_atual = row.get('usuario', '')
                    retry_api(ws_l.update, range_name=f"C{linha}:F{linha}", values=[["Concluído", usr_atual, prog_str, ""]])
                else:
                    vals = [prog_str]
                    rg = f"E{linha}"
                    if checkpoint_val: 
                        vals.append(checkpoint_val) 
                        rg = f"E{linha}:F{linha}"
                    retry_api(ws_l.update, range_name=rg, values=[vals])
                break
    
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
        try:
            ws.append_row([str(uuid.uuid4()), str(num_lote), ini.strftime("%Y-%m-%d"), str(usuario), ini.strftime("%H:%M:%S"), fim.strftime("%H:%M:%S"), int(duracao), str(nome_proj), f"{acao} ({feitos}/{total})"])
        except: pass
    except: pass

def baixar_excel(id_p):
    try:
        client = get_client_coleta()
        ss = abrir_planilha(client)
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
    except: return None

def gerar_modelo_padrao():
    df = pd.DataFrame(columns=["Site*", "Descrição*", "EAN*", "Quantidade no Lote*", "CEP", "Endereço"])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
    return out.getvalue()