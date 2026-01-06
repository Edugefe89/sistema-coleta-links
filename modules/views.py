import streamlit as st
import pandas as pd
import time
from datetime import datetime
from modules import services, ui

# --- TELA DE LOGIN ---
def tela_login(senhas):
    if 'usuario_logado_temp' in st.session_state: 
        return st.session_state['usuario_logado_temp']

    st.title("🔒 Acesso Restrito")
    c1, _ = st.columns([2,1])
    with c1:
        with st.form("login"):
            usr = st.selectbox("Usuário", ["Selecione..."] + list(senhas.keys()))
            pwd = st.text_input("Senha", type="password")
            
            if st.form_submit_button("Entrar", type="primary"):
                if usr != "Selecione..." and pwd == senhas[usr]:
                    st.session_state['usuario_logado_temp'] = usr
                    st.rerun()
                else: 
                    st.error("Senha incorreta.")
    st.stop()

# --- TELA ADMIN ---
def tela_admin():
    st.markdown("## ⚙️ Painel Admin")
    t1, t2 = st.tabs(["Novo Projeto", "Relatórios"])
    with t1:
        st.markdown("### 1. Baixar Modelo")
        st.download_button("📥 Modelo Excel", services.gerar_modelo_padrao(), "modelo.xlsx")
        st.markdown("### 2. Enviar")
        with st.form("upload"):
            arq = st.file_uploader("Excel (.xlsx)", type=["xlsx"])
            if st.form_submit_button("🚀 Criar", type="primary") and arq:
                try:
                    df = pd.read_excel(arq, dtype=str)
                    with st.spinner("Enviando..."):
                        id_p, q, t = services.processar_upload(df, arq.name)
                        if id_p:
                            st.success(f"Sucesso! ID: {id_p} | Lotes: {int(q/t) + 1}")
                            st.balloons()
                except Exception as e: st.error(f"Erro ao processar: {e}")

    with t2:
        projs = services.carregar_projetos_ativos()
        if not projs.empty:
            p_dict = {r['nome']: r['id'] for _, r in projs.iterrows()}
            sel = st.selectbox("Projeto:", list(p_dict.keys()))
            if st.button("📦 Gerar Excel"):
                with st.spinner("Baixando..."):
                    dado = services.baixar_excel(p_dict[sel])
                    if dado: st.download_button("📥 Download", dado, f"{sel}.xlsx")
                    else: st.error("Erro ao baixar.")

# --- FRAGMENTO DA TABELA (CORRIGIDO PARA NÃO PULAR SCROLL) ---
@st.fragment
def fragmento_tabela(id_p, lote, user, nome_p):
    if 'df_cache' not in st.session_state:
        st.error("Erro de estado. Recarregue (F5).")
        return
    
    # PEGA O ORIGINAL (Sem cópia!) - Isso mantém o Scroll fixo
    df_ref = st.session_state['df_cache']

    # Injeta a coluna de Busca no Próprio Original (se não tiver)
    # Isso evita ter que recriar o dataframe todo
    if 'BUSCA_GOOGLE' not in df_ref.columns:
        df_ref['BUSCA_GOOGLE'] = df_ref.apply(lambda x: f"https://www.google.com/search?q={x['ean']}", axis=1)

    # --- CALLBACK DE SALVAMENTO ---
    def callback_salvar():
        changes = st.session_state["editor_links"].get("edited_rows", {})
        if not changes: return

        lista_para_salvar = []
        
        for idx_str, val in changes.items():
            idx = int(idx_str)
            if "link" in val:
                novo_link = val["link"]
                
                # Atualiza memória local (No objeto original)
                df_ref.at[idx, 'link'] = novo_link
                
                # Prepara envio
                linha_excel = int(df_ref.iloc[idx]['_row_index'])
                lista_para_salvar.append({
                    'indice_excel': linha_excel,
                    'link': novo_link
                })
                
                if 'saved_indices' not in st.session_state: st.session_state['saved_indices'] = set()
                if novo_link and str(novo_link).strip() != "":
                    st.session_state['saved_indices'].add(idx)
                else:
                    st.session_state['saved_indices'].discard(idx)

        # Envia em Lote
        if lista_para_salvar:
            sucesso = services.salvar_lote_links(id_p, lote, lista_para_salvar)
            if sucesso:
                st.toast("Salvo!", icon="☁️")
            else:
                st.toast("Erro ao salvar.", icon="❌")

    col_t, _ = st.columns([1,4])
    foco = col_t.toggle("🎯 Modo Foco")
    
    # Define quais colunas mostrar usando column_order (sem cortar o DF)
    cols_ordem = ['ean', 'descricao', 'BUSCA_GOOGLE', 'link']
    if 'MARCADOR' in df_ref.columns: cols_ordem.insert(0, 'MARCADOR')
    
    # Preparação para exibição
    if foco:
        # No modo foco, infelizmente precisamos criar uma view filtrada
        # (O scroll pode resetar se linhas sumirem, mas é esperado no filtro)
        mask = (df_ref['link'] == "") | (df_ref['link'].isna())
        df_show = df_ref[mask]
    else:
        # No modo normal, usamos o ORIGINAL. Scroll fica fixo. ⚓
        df_show = df_ref

    st.data_editor(
        df_show,
        key="editor_links",
        on_change=callback_salvar,
        column_order=cols_ordem, # <--- O SEGREDO ESTÁ AQUI
        column_config={
            "MARCADOR": st.column_config.TextColumn("Marcador", disabled=True, width="small"),
            "ean": st.column_config.TextColumn("EAN", disabled=True, width="medium"),
            "descricao": st.column_config.TextColumn("Descrição", disabled=True),
            "BUSCA_GOOGLE": st.column_config.LinkColumn("Ajuda", display_text="🔍 Google", width="small"),
            "link": st.column_config.LinkColumn("Link Coletado", validate="^https?://", width="large")
        },
        hide_index=True, use_container_width=True, height=600,
        num_rows="fixed"
    )

    if 'saved_indices' not in st.session_state: st.session_state['saved_indices'] = set()
    preenchidos = df_ref[df_ref['link'].astype(str).str.strip() != ""].shape[0]
    total = len(df_ref)
    
    st.progress(int((preenchidos/total)*100) if total > 0 else 0, f"Progresso: {preenchidos}/{total}")
    
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        opcoes = ["Nada"] + df_ref['descricao'].tolist()
        try: idx = opcoes.index(st.session_state.get('last_check', '')) 
        except: idx = 0
        sel = st.selectbox("Pausar em:", opcoes, index=idx, key="sel_pausa_frag")
        
        if st.button("💾 Salvar e Pausar"):
            check = sel if sel != "Nada" else ""
            with st.spinner("Saindo..."):
                services.salvar_progresso_lote(df_ref, id_p, lote, False, check) 
                tempo = (datetime.now(services.TZ_BRASIL) - st.session_state['h_ini']).total_seconds()
                services.salvar_log_tempo(user, id_p, nome_p, lote, tempo, "Pausa", total, preenchidos)
                st.session_state['status'] = 'PAUSADO'
                if 'df_cache' in st.session_state: del st.session_state['df_cache']
                if 'saved_indices' in st.session_state: del st.session_state['saved_indices']
                st.rerun()

    with c2:
        if st.button("✅ Entregar Lote", type="primary"):
            with st.spinner("Finalizando..."):
                services.salvar_progresso_lote(df_ref, id_p, lote, True)
                tempo = (datetime.now(services.TZ_BRASIL) - st.session_state['h_ini']).total_seconds()
                services.salvar_log_tempo(user, id_p, nome_p, lote, tempo, "Fim", total, preenchidos)
                for k in ['lote_ativo', 'h_ini', 'status', 'df_cache', 'saved_indices']: 
                    if k in st.session_state: del st.session_state[k]
                st.balloons(); time.sleep(1); st.rerun()

# --- TELA PRODUÇÃO ---
def tela_producao(user):
    st.title(f"🏭 Produção | {user}")
    projs = services.carregar_projetos_ativos()
    if projs.empty: st.info("Sem projetos."); return

    p_dict = {r['nome']: r['id'] for _, r in projs.iterrows()}
    nome_p = st.selectbox("Projeto:", ["Selecione..."] + list(p_dict.keys()), key="sb_p")
    if nome_p == "Selecione...": st.stop()
    id_p = p_dict[nome_p]

    df_lotes = services.carregar_lotes_do_projeto(id_p)
    with st.expander("📊 Mapa Geral"):
        st.dataframe(df_lotes[['usuario', 'lote', 'status']], hide_index=True)
    
    st.divider()
    
    if 'lote_ativo' not in st.session_state:
        meus = df_lotes[(df_lotes['status']=='Em Andamento')&(df_lotes['usuario']==user)]['lote'].unique()
        livres = df_lotes[df_lotes['status']=='Livre']['lote'].unique()
        
        meus = sorted([int(x) for x in meus])
        livres = sorted([int(x) for x in livres])
        
        opts = [f"Lote {l} (RETOMAR)" for l in meus] + [f"Lote {l} (NOVO)" for l in livres]
        
        c1, c2 = st.columns([3,1])
        sel = c1.selectbox("Trabalho:", ["Selecione..."]+opts, key="sb_l")
        if sel != "Selecione..." and c2.button("Acessar", type="primary"):
            num = int(sel.split()[1])
            if "NOVO" in sel:
                if not services.reservar_lote(id_p, num, user):
                    st.error("Erro ao reservar."); time.sleep(2); st.rerun()
            
            st.session_state.update({'lote_ativo': num, 'status': 'TRABALHANDO', 'h_ini': datetime.now(services.TZ_BRASIL)})
            # Limpa caches antigos ao trocar de lote
            if 'df_cache' in st.session_state: del st.session_state['df_cache']
            if 'saved_indices' in st.session_state: del st.session_state['saved_indices']
            st.rerun()
    else:
        lote = st.session_state['lote_ativo']
        
        if 'df_cache' not in st.session_state:
            df = services.carregar_dados_lote(id_p, lote)
            
            if df.empty:
                st.error("Erro ao carregar dados. Limpe o cache.")
                if st.button("Voltar"): 
                    del st.session_state['lote_ativo']
                    st.rerun()
                st.stop()

            chk = ""
            info = df_lotes[df_lotes['lote']==str(lote)]
            if not info.empty: 
                raw = info.iloc[0]['checkpoint']
                if str(raw) not in ["nan", ""]: chk = str(raw).strip()
            
            df.insert(0, "MARCADOR", "")
            if chk: 
                mask = df['descricao'].astype(str).str.strip() == chk
                df.loc[mask, 'MARCADOR'] = ">>> PAREI AQUI <<<"
                st.session_state['last_check'] = chk
            st.session_state['df_cache'] = df
        
        df_header = st.session_state['df_cache']
        if not df_header.empty:
            site_val = df_header.iloc[0]['site'] if 'site' in df_header.columns else '-'
            cep_val = df_header.iloc[0]['cep'] if 'cep' in df_header.columns else '-'
            end_val = df_header.iloc[0]['endereco'] if 'endereco' in df_header.columns else '-'
            ui.render_header_lote(lote, site_val, cep_val, end_val)
        
        if st.session_state.get('status') == 'PAUSADO':
            st.warning(f"⏸️ Lote {lote} Pausado"); 
            if st.button("▶️ VOLTAR"): st.session_state['status'] = 'TRABALHANDO'; st.session_state['h_ini'] = datetime.now(services.TZ_BRASIL); st.rerun()
        else:
            fragmento_tabela(id_p, lote, user, nome_p)