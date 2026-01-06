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

# --- AUTENTICAÇÃO OTIMIZADA (CACHE) ---
# AQUI ESTÁ A MÁGICA: @st.cache_resource
# Isso mantém a conexão aberta na memória RAM do servidor por 30 min (ttl=1800)
@st.cache_resource(ttl=1800)
def get_conexao_cached():
    print("🔄 (RE)CONECTANDO AO GOOGLE SHEETS...")
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_dict = dict(st.secrets["connections"]["gsheets_coleta"])
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        client = gspread.authorize(creds)
        
        # Abre a planilha e retorna o OBJETO PRONTO
        return client.open_by_key(ID_PLANILHA_COLETA)
    except Exception as e:
        st.error(f"Erro fatal de conexão: {e}")
        return None

# Função Wrapper para manter compatibilidade com o código antigo
def get_client_coleta():
    # Retorna qualquer coisa True, pois a conexão real está no abrir_planilha agora
    return True 

def abrir_planilha(client_ignorado=None):
    # Ignora o argumento 'client' e pega a conexão quente do Cache
    return get_conexao_cached()

# --- LEITURA ---
def carregar_projetos_ativos():
    try:
        ss = abrir_planilha()
        ws = ss.worksheet("projetos")
        data = retry_api(ws.get_all_records)
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        return df[df['status'] == 'Ativo'] if not df.empty else df
    except: return pd.DataFrame()

def carregar_lotes_do_projeto(id_projeto):
    try:
        ss = abrir_planilha()
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
        ss = abrir_planilha()
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

# --- UPLOAD ---
def processar_upload(df, nome_arq):
    st.divider()
    st.markdown("### 🛠️ UPLOAD COM CORREÇÃO DE POSIÇÃO")

    try:
        ss = abrir_planilha()
        if ss is None: raise Exception("Falha Auth.")
        
        # --- TRATAMENTO ---
        df = df.astype(str)
        for termo in ["nan", "None", "NaT", "<NA>"]:
            df = df.replace(termo, "")

        if len(df.columns) < 3:
            st.error("❌ Excel inválido (poucas colunas).")
            return None, 0, 0

        id_p = str(uuid.uuid4())[:8]
        
        # Tamanho do lote
        tam = 100
        try:
            if len(df.columns) > 3:
                val = df.iloc[0, 3]
                if val and val.strip(): tam = int(float(val))
        except: tam = 100
        
        total_lotes = (len(df) // tam) + (1 if len(df) % tam > 0 else 0)
        l_dados, l_lotes = [], []

        # MONTAGEM DA LISTA
        for i in range(total_lotes):
            num = i + 1
            sub = df.iloc[i*tam : (i+1)*tam]
            for _, r in sub.iterrows():
                d_site = str(r.iloc[0]).strip()
                d_desc = str(r.iloc[1]).strip()
                d_ean  = str(r.iloc[2]).strip()
                d_cep  = str(r.iloc[4]).strip() if len(r) > 4 else ""
                d_end  = str(r.iloc[5]).strip() if len(r) > 5 else ""
                
                if d_site == "" and l_dados: d_site = l_dados[-1][4]

                l_dados.append([id_p, num, d_ean, d_desc, d_site, d_cep, d_end, ""])
            l_lotes.append([id_p, num, "Livre", "", f"0/{len(sub)}", ""])
            
        # --- GRAVAÇÃO ---
        st.write("🚀 Gravando abas de controle...")
        retry_api(ss.worksheet("projetos").append_row, [id_p, nome_arq.replace(".xlsx",""), datetime.now(TZ_BRASIL).strftime("%d/%m/%Y"), int(total_lotes), "Ativo"])
        retry_api(ss.worksheet("controle_lotes").append_rows, l_lotes)
        
        if l_dados:
            st.write(f"⏳ Calculando posição correta para {len(l_dados)} linhas...")
            ws_dados = ss.worksheet("dados_brutos")
            
            # 1. Descobre a última linha (coluna A)
            col_a = retry_api(ws_dados.col_values, 1) 
            prox_linha = len(col_a) + 1
            
            # 2. Define o Range
            linha_final = prox_linha + len(l_dados) - 1
            range_destino = f"A{prox_linha}:H{linha_final}"
            
            st.write(f"📍 Gravando forçadamente em: `{range_destino}`")
            
            # 3. Update
            retry_api(ws_dados.update, range_name=range_destino, values=l_dados)
            
            st.success("✅ DADOS SALVOS NAS COLUNAS CERTAS (A-H)!")
        
        return id_p, len(df), tam

    except Exception as e:
        st.error(f"❌ ERRO: {e}")
        traceback.print_exc()
        raise e
    
def gerar_modelo_padrao():
    colunas = ["Site*", "Descrição*", "EAN*", "Quantidade no Lote*", "CEP", "Endereço"]
    df = pd.DataFrame(columns=colunas)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer: df.to_excel(writer, index=False)
    return out.getvalue()

# --- FUNÇÕES DE ESCRITA ---

def reservar_lote(id_projeto, numero_lote, usuario):
    try:
        ss = abrir_planilha()
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

# ⚠️ SALVAMENTO EM LOTE COM CONEXÃO CACHEADA
def salvar_lote_links(id_projeto, numero_lote, alteracoes):
    if not alteracoes: return True

    try:
        ss = abrir_planilha() # USA O CACHE, NÃO BATE NA API
        ws = ss.worksheet("dados_brutos")
        
        batch_data = []
        for item in alteracoes:
            linha = item['indice_excel'] 
            link = item['link']
            batch_data.append({
                'range': f"H{linha}",
                'values': [[link]]
            })
        
        if batch_data:
            retry_api(ws.batch_update, batch_data)
            return True
    except Exception as e:
        print(f"Erro ao salvar lote: {e}")
        return False
    return True

def salvar_progresso_lote(df_editado, id_projeto, numero_lote, concluir=False, checkpoint_val=""):
    ss = abrir_planilha() # USA O CACHE
    ws_d = ss.worksheet("dados_brutos")
    ws_l = ss.worksheet("controle_lotes")
    
    updates = []
    
    # 1. SANITIZAÇÃO
    df_safe = df_editado.copy()
    df_safe['link'] = df_safe['link'].fillna("")
    
    if '_row_index' in df_safe.columns:
        for _, row in df_safe.iterrows():
            try:
                linha = int(row['_row_index'])
                link_val = str(row['link']) if row['link'] else ""
                updates.append({
                    'range': f'H{linha}', 
                    'values': [[link_val]]
                })
            except: continue
    else:
        # Fallback
        todos = retry_api(ws_d.get_all_records)
        if todos:
            mapa = {}
            for i, row in enumerate(todos):
                rid = str(row.get('id_projeto', list(row.values())[0]))
                rlote = str(row.get('lote', list(row.values())[1]))
                rean = str(row.get('ean', list(row.values())[2]))
                if rid == str(id_projeto) and rlote == str(numero_lote):
                    mapa[rean] = i + 2
            for _, row in df_safe.iterrows():
                linha = mapa.get(str(row['ean']))
                link_val = str(row['link']) if row['link'] else ""
                if linha: 
                    updates.append({
                        'range': f'H{linha}', 
                        'values': [[link_val]]
                    })

    # 2. ENVIAR DADOS
    if updates:
        try:
            retry_api(ws_d.batch_update, updates)
        except Exception as e:
            print(f"Erro ao salvar links finais: {e}")

    # 3. ATUALIZAR STATUS
    preenchidos = len(df_safe[df_safe['link'].str.strip() != ""])
    prog_str = f"{preenchidos}/{len(df_safe)}"
    
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
        ss = abrir_planilha()
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

# --- DOWNLOAD BLINDADO ---
def baixar_excel(id_p):
    print(f"--- 📥 INICIANDO DOWNLOAD DO PROJETO {id_p} ---")
    try:
        ss = abrir_planilha()
        ws = ss.worksheet("dados_brutos")
        data = retry_api(ws.get_all_values)
        
        if not data or len(data) < 2: 
            print("❌ Planilha vazia ou sem dados.")
            return None

        headers = [str(h).lower().strip() for h in data[0]]
        df = pd.DataFrame(data[1:], columns=headers)
        df_filtrado = df[df['id_projeto'].astype(str) == str(id_p)].copy()
        
        if df_filtrado.empty: return None

        colunas_finais = {
            'ean': 'EAN',
            'descricao': 'Descrição do Produto',
            'site': 'Site/Loja',
            'link': 'LINK COLETADO',
            'cep': 'CEP',
            'endereco': 'Endereço',
            'lote': 'Lote de Origem'
        }
        
        cols_existentes = [c for c in colunas_finais.keys() if c in df_filtrado.columns]
        df_final = df_filtrado[cols_existentes].rename(columns=colunas_finais)
        
        if 'Lote de Origem' in df_final.columns:
            try:
                df_final['Lote de Origem'] = pd.to_numeric(df_final['Lote de Origem'])
                df_final = df_final.sort_values(by=['Lote de Origem'])
            except: pass

        out = io.BytesIO()
        with pd.ExcelWriter(out, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False)
            ws_out = writer.sheets['Sheet1']
            ws_out.column_dimensions['B'].width = 50 
            ws_out.column_dimensions['D'].width = 60 
            
        print("✅ Excel gerado com sucesso!")
        return out.getvalue()
        
    except Exception as e:
        print(f"❌ Erro crítico no download: {e}")
        return None