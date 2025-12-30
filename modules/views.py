import streamlit as st
import pandas as pd
import time
from datetime import datetime
from modules import services, ui

# --- TELA DE LOGIN ---
def tela_login(senhas):
    if 'usuario_logado_temp' in st.session_state: return st.session_state['usuario_logado_temp']
    cm = services.get_manager()
    c_usr = cm.get(cookie="usuario_coleta")
    if c_usr: st.session_state['usuario_logado_temp'] = c_usr; return c_usr

    st.title("🔒 Acesso Restrito")
    c1, _ = st.columns([2,1])
    with c1:
        with st.form("login"):
            usr = st.selectbox("Usuário", ["Selecione..."] + list(senhas.keys()))
            pwd = st.text_input("Senha", type="password")
            if st.form_submit_button("Entrar", type="primary"):
                if usr != "Selecione..." and pwd == senhas[usr]:
                    st.session_state['usuario_logado_temp'] = usr
                    cm.set("usuario_coleta", usr, expires_at=datetime.now()+pd.Timedelta(days=1))
                    st.rerun()
                else: st.error("Senha incorreta.")
    st.stop()

# --- TELA ADMIN (LIMPA) ---
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
                    df.columns = [services.remove_accents(str(c).lower().strip().replace(" ","")) for c in df.columns]
                    if all(c in df.columns for c in ['site*', 'descricao*', 'ean*', 'quantidadenolote*']):
                        with st.spinner("Enviando..."):
                            id_p, q, t = services.processar_upload(df, arq.name)
                            st.success(f"Sucesso! ID: {id_p}"); st.balloons()
                    else: st.error("Colunas obrigatórias faltando (com *).")
                except Exception as e: st.error(f"Erro ao processar: {e}")

    with t2:
        projs = services.carregar_projetos_ativos()
        if not projs.empty:
            p_dict = {r['nome']: r['id'] for _, r in projs.iterrows()}
            sel = st.selectbox("Projeto:", list(p_dict.keys()))
            if st.button("📦 Gerar Excel"):
                with st.spinner("Baixando..."):
                    st.download_button("📥 Download", services.baixar_excel(p_dict[sel]), f"{sel}.xlsx")

# --- O FRAGMENTO BLINDADO ANTI-SCROLL ---
@st.fragment
def fragmento_tabela(id_p, lote, user, nome_p):
    if 'df_cache' not in st.session_state:
        st.error("Erro. Recarregue.")
        return
    
    df_ref = st.session_state['df_cache']

    def callback_salvar():
        changes = st.session_state["editor_links"].get("edited_rows", {})
        count_saves = 0
        for idx, val in changes.items():
            if "link" in val:
                novo_link = val["link"]
                idx_int = int(idx)
                services.salvar_alteracao_individual(id_p, lote, idx_int, novo_link, df_ref)
                
                if 'saved_indices' not in st.session_state: st.session_state['saved_indices'] = set()
                if novo_link.strip() != "":
                    st.session_state['saved_indices'].add(idx_int)
                else:
                    st.session_state['saved_indices'].discard(idx_int)
                count_saves += 1
        
        if count_saves > 0:
            st.toast("Link salvo!", icon="☁️")

    col_t, _ = st.columns([1,4])
    foco = col_t.toggle("🎯 Modo Foco (Ocultar Prontos)")
    
    df_visual = df_ref[['MARCADOR', 'ean', 'descricao', 'BUSCA_GOOGLE', 'link']].copy()
    if foco:
        df_visual = df_visual[(df_visual['link'] == "") | (df_visual['link'].isna())]

    st.data_editor(
        df_visual,
        key="editor_links",
        on_change=callback_salvar,
        column_config={
            "MARCADOR": st.column_config.TextColumn("Marcador", disabled=True, width="small"),
            "ean": st.column_config.TextColumn("EAN", disabled=True, width="small"),
            "descricao": st.column_config.TextColumn("Descrição", disabled=True),
            "BUSCA_GOOGLE": st.column_config.LinkColumn("Ajuda", display_text="🔍 Buscar", width="small"),
            "link": st.column_config.LinkColumn(
                "Link Coletado", 
                validate="^https?://", 
                width="large",
                help="Cole aqui. O salvamento é automático."
            )
        },
        hide_index=True, use_container_width=True, height=600,
        num_rows="fixed" if not foco else "dynamic"
    )

    if 'saved_indices' not in st.session_state: st.session_state['saved_indices'] = set()
    feitos_originais = df_ref[df_ref['link'].astype(str).str.strip() != ""].index.tolist()
    todos_feitos = set(feitos_originais) | st.session_state['saved_indices']
    
    tot = len(df_ref)
    feitos_count = len(todos_feitos)
    
    st.progress(int((feitos_count/tot)*100) if tot > 0 else 0, f"Progresso: {feitos_count}/{tot}")
    
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
                services.salvar_log_tempo(user, id_p, nome_p, lote, tempo, "Pausa", tot, feitos_count)
                st.session_state['status'] = 'PAUSADO'
                if 'df_cache' in st.session_state: del st.session_state['df_cache']
                if 'saved_indices' in st.session_state: del st.session_state['saved_indices']
                st.rerun()

    with c2:
        if st.button("✅ Entregar Lote", type="primary"):
            with st.spinner("Finalizando..."):
                services.salvar_progresso_lote(df_ref, id_p, lote, True)
                tempo = (datetime.now(services.TZ_BRASIL) - st.session_state['h_ini']).total_seconds()
                services.salvar_log_tempo(user, id_p, nome_p, lote, tempo, "Fim", tot, feitos_count)
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
        opts = [f"Lote {l} (RETOMAR)" for l in sorted(meus)] + [f"Lote {l} (NOVO)" for l in sorted(livres)]
        
        c1, c2 = st.columns([3,1])
        sel = c1.selectbox("Trabalho:", ["Selecione..."]+opts, key="sb_l")
        if sel != "Selecione..." and c2.button("Acessar", type="primary"):
            num = int(sel.split()[1])
            if num in livres and not services.reservar_lote(id_p, num, user):
                st.error("Erro."); time.sleep(1); st.rerun()
            st.session_state.update({'lote_ativo': num, 'status': 'TRABALHANDO', 'h_ini': datetime.now(services.TZ_BRASIL)})
            if 'df_cache' in st.session_state: del st.session_state['df_cache']
            if 'saved_indices' in st.session_state: del st.session_state['saved_indices']
            st.rerun()
    else:
        lote = st.session_state['lote_ativo']
        
        if 'df_cache' not in st.session_state:
            df = services.carregar_dados_lote(id_p, lote)
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
            
            df['BUSCA_GOOGLE'] = df.apply(lambda x: f"https://www.google.com/search?q={x['ean']}+{x['descricao']}".replace(" ","+"), axis=1)
            st.session_state['df_cache'] = df
        
        df_header = st.session_state['df_cache']
        if not df_header.empty:
            ui.render_header_lote(lote, df_header.iloc[0]['site'], df_header.iloc[0]['cep'], df_header.iloc[0]['endereco'])
        
        if st.session_state.get('status') == 'PAUSADO':
            st.warning(f"⏸️ Lote {lote} Pausado"); 
            if st.button("▶️ VOLTAR"): st.session_state['status'] = 'TRABALHANDO'; st.session_state['h_ini'] = datetime.now(services.TZ_BRASIL); st.rerun()
        else:
            fragmento_tabela(id_p, lote, user, nome_p)