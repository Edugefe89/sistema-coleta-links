import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
import uuid
import time
import io
import unicodedata
import random
import traceback

# --- CONFIGURAÇÃO ---
TZ_BRASIL = timezone(timedelta(hours=-3))
ID_PLANILHA_COLETA = "1IwV0h5HrqBkl4owb3lVzPIl2lLxj9n3cfH15U_SISlQ" 

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

# --- RETRY API ---
def retry_api(func, *args, **kwargs):
    max_tentativas = 5
    for i in range(max_tentativas):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == max_tentativas - 1:
                print(f"❌ Erro fatal API: {e}")
                raise e 
            wait_time = (1.5 ** i) + random.uniform(0, 1) 
            time.sleep(wait_time)
    return None

# --- AUTENTICAÇÃO ---
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

# --- LEITURA ---
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
        raw_data = retry_api(ws.get_all_values)
        if not raw_data or len(raw_data) < 2: return pd.DataFrame()
        
        headers = raw_data.pop(0) 
        df = pd.DataFrame(raw_data, columns=headers)
        df.columns = [str(c).lower().strip() for c in df.columns]

        if '_row_index' not in df.columns:
            df['_row_index'] = range(2, len(df) + 2)
            
        cols_essenciais = ["id_projeto", "lote", "ean", "descricao", "site", "cep", "endereco", "link", "_row_index"]
        for col in cols_essenciais:
            if col not in df.columns: df[col] = "" 
        
        if not df.empty:
            df = df[cols_essenciais]
            df['id_projeto'] = df['id_projeto'].astype(str)
            df['lote'] = df['lote'].astype(str)
            return df[(df['id_projeto'] == str(id_projeto)) & (df['lote'] == str(numero_lote))]
        return df
    except Exception as e:
        print(f"Erro carregar dados: {e}")
        return pd.DataFrame()

# --- FUNÇÕES ESPECIAIS DE UPLOAD (AJUSTADAS) ---

def processar_upload(df, nome_arq):
    print("\n" + "="*50)
    print("🕵️‍♂️ INICIANDO DEBUG DE UPLOAD (VAMOS PEGAR ESSE ERRO)")
    print("="*50)
    
    try:
        client = get_client_coleta()
        if client is None: 
            print("❌ ERRO FATAL: Cliente do Google retornou None (Falha na Autenticação)")
            raise Exception("Falha Auth.")
        print("✅ Cliente Google Autenticado")

        ss = abrir_planilha(client)
        print(f"✅ Planilha Acessada: {ss.title}")
        
        # DEBUG 1: O QUE CHEGOU DO EXCEL?
        print(f"📊 DataFrame Recebido: {len(df)} linhas")
        print(f"📊 Colunas Originais: {list(df.columns)}")
        
        # Tratamento básico
        df = df.astype(str)
        for termo in ["nan", "None", "NaT", "<NA>"]:
            df = df.replace(termo, "")
            
        # VALIDAÇÃO POR POSIÇÃO (ÍNDICE)
        # 0: Site | 1: Descrição | 2: EAN | 3: Qtd | 4: CEP | 5: Endereço
        if len(df.columns) < 3:
            print(f"❌ ERRO: O Excel tem poucas colunas ({len(df.columns)}). Abortando.")
            raise Exception("Excel inválido")

        # DEBUG 2: O QUE TEM NA PRIMEIRA LINHA?
        print(f"🔎 Amostra da Linha 0 (Crua): {df.iloc[0].tolist()}")

        id_p = str(uuid.uuid4())[:8]
        
        # Tamanho do lote
        tam = 100
        try:
            if len(df.columns) > 3:
                val = df.iloc[0, 3] # Coluna 3
                if val and val.strip(): tam = int(float(val))
        except Exception as e:
            print(f"⚠️ Aviso: Falha ao ler tamanho do lote ({e}). Usando 100.")
            tam = 100
        
        total_lotes = (len(df) // tam) + (1 if len(df) % tam > 0 else 0)
        l_dados, l_lotes = [], []
        
        print(f"⚙️ Processando {total_lotes} lotes...")

        for i in range(total_lotes):
            num = i + 1
            sub = df.iloc[i*tam : (i+1)*tam]
            for idx_row, r in sub.iterrows():
                # MONTAGEM DOS DADOS (POR POSIÇÃO)
                try:
                    # Tenta pegar por índice seguro
                    dado_site = str(r.iloc[0]).strip()
                    dado_desc = str(r.iloc[1]).strip()
                    dado_ean  = str(r.iloc[2]).strip()
                    dado_cep  = str(r.iloc[4]).strip() if len(r) > 4 else ""
                    dado_end  = str(r.iloc[5]).strip() if len(r) > 5 else ""
                    
                    # Preenche contexto (repetir site se vazio na mesma sequência)
                    if dado_site == "" and len(l_dados) > 0:
                        # Pega o site do último registro adicionado (lógica simples de ffill)
                        dado_site = l_dados[-1][4] 

                    linha_para_gravar = [
                        id_p,       # id_projeto
                        num,        # lote
                        dado_ean,   # ean
                        dado_desc,  # descricao
                        dado_site,  # site
                        dado_cep,   # cep
                        dado_end,   # endereco
                        ""          # link
                    ]
                    l_dados.append(linha_para_gravar)
                except Exception as e:
                    print(f"❌ Erro ao processar linha {idx_row}: {e}")

            l_lotes.append([id_p, num, "Livre", "", f"0/{len(sub)}", ""])
            
        # DEBUG 3: A LISTA FINAL
        print(f"📦 Total Lotes Gerados: {len(l_lotes)}")
        print(f"📦 Total Dados Gerados: {len(l_dados)}")
        
        if len(l_dados) > 0:
            print(f"🔎 Amostra do 1º dado a gravar: {l_dados[0]}")
        else:
            print("❌ ERRO CRÍTICO: A lista 'l_dados' está VAZIA. O loop falhou.")
        
        # GRAVAÇÃO COM LOG EXPLÍCITO
        print("🚀 Gravando PROJETOS...")
        retry_api(ss.worksheet("projetos").append_row, [id_p, nome_arq.replace(".xlsx",""), datetime.now(TZ_BRASIL).strftime("%d/%m/%Y"), int(total_lotes), "Ativo"])
        
        print("🚀 Gravando CONTROLE_LOTES...")
        retry_api(ss.worksheet("controle_lotes").append_rows, l_lotes)
        
        print("🚀 Gravando DADOS_BRUTOS...")
        if l_dados:
            try:
                # Tenta gravar
                res = retry_api(ss.worksheet("dados_brutos").append_rows, l_dados)
                print(f"✅ SUCESSO! Resposta da API: {res}")
            except Exception as e:
                print("❌❌❌ ERRO AO GRAVAR NO GOOGLE SHEETS ❌❌❌")
                print(f"Erro: {e}")
                traceback.print_exc() # Imprime o erro completo
                raise e
        else:
            print("⚠️ Pulei a gravação de dados brutos porque a lista estava vazia.")

        print("🏁 FIM DO PROCESSO DE DEBUG")
        print("="*50 + "\n")
        
        return id_p, len(df), tam

    except Exception as e:
        print(f"❌ ERRO GERAL NA FUNÇÃO: {e}")
        traceback.print_exc()
        raise e
    
# --- MODELO EXCEL (COM OS NOMES BONITOS QUE VOCÊ PEDIU) ---
def gerar_modelo_padrao():
    # Headers exatamente como você solicitou
    colunas = ["Site*", "Descrição*", "EAN*", "Quantidade no Lote*", "CEP", "Endereço"]
    
    df = pd.DataFrame(columns=colunas)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
    return out.getvalue()

# --- DEMAIS FUNÇÕES DE ESCRITA ---

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