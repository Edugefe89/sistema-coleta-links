import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import uuid

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="Sistema Coleta", layout="wide")

# --- CONEXÃO COM O GOOGLE ---
@st.cache_resource
def get_client_google():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        # Usa a mesma estrutura de segredos do projeto anterior
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

def processar_upload_lotes(df, nome_arquivo):
    client = get_client_google()
    ss = client.open("Sistema_Coleta_Links") # Nome da NOVA planilha
    
    # Abas
    ws_projetos = ss.worksheet("projetos")
    ws_lotes = ss.worksheet("controle_lotes")
    ws_dados = ss.worksheet("dados_brutos")
    
    # 1. Preparar Dados do Projeto
    id_projeto = str(uuid.uuid4())[:8] # ID curto
    data_hoje = datetime.now().strftime("%d/%m/%Y")
    total_linhas = len(df)
    tamanho_lote = 100
    total_lotes = (total_linhas // tamanho_lote) + (1 if total_linhas % tamanho_lote > 0 else 0)
    
    # 2. Criar Listas para Upload em Massa
    lista_dados_brutos = []
    lista_controle_lotes = []
    
    for i in range(total_lotes):
        num_lote = i + 1
        inicio = i * tamanho_lote
        fim = inicio + tamanho_lote
        df_lote = df.iloc[inicio:fim]
        
        # Prepara linhas da aba DADOS_BRUTOS
        for _, row in df_lote.iterrows():
            # Tenta pegar EAN e Descricao de forma segura
            ean_val = row['ean'] if 'ean' in row else row.iloc[1] # fallback index
            desc_val = row['descricao'] if 'descricao' in row else row.iloc[0] # fallback index
            
            lista_dados_brutos.append([
                id_projeto,
                num_lote,
                str(ean_val), 
                desc_val,
                "" # Link vazio
            ])
            
        # Prepara linha da aba CONTROLE_LOTES
        lista_controle_lotes.append([
            id_projeto,
            num_lote,
            "Livre",
            "",
            f"0/{len(df_lote)}"
        ])

    # 3. Executar Upload
    try:
        with st.spinner("Enviando dados para o Google Sheets..."):
            ws_projetos.append_row([id_projeto, nome_arquivo, data_hoje, total_lotes, "Ativo"])
            ws_lotes.append_rows(lista_controle_lotes)
            ws_dados.append_rows(lista_dados_brutos)
            return True, total_lotes, id_projeto
        
    except Exception as e:
        st.error(f"Erro no envio para o Google: {e}")
        return False, 0, 0

# --- INTERFACE ADMIN ---
st.title("📤 Upload de Novos Projetos")

arquivo = st.file_uploader("Suba o Excel (Colunas: ean, descricao)", type=["xlsx"])

if arquivo:
    try:
        df = pd.read_excel(arquivo)
        # Normaliza colunas para minusculo
        df.columns = [str(c).lower().strip() for c in df.columns]
        
        st.write("Pré-visualização:", df.head())
        
        if st.button("🚀 Processar Lotes"):
            sucesso, qtd_lotes, id_proj = processar_upload_lotes(df, arquivo.name)
            if sucesso:
                st.balloons()
                st.success(f"Projeto `{id_proj}` criado com **{qtd_lotes} lotes**!")
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {e}")
