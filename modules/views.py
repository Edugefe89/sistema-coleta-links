import streamlit as st
import pandas as pd
import time
from datetime import datetime
from modules import services, ui

# --- TELA DE LOGIN ---
def tela_login(usuarios):
    if 'usuario_logado_temp' in st.session_state:
        return st.session_state['usuario_logado_temp']

    cookie_manager = services.get_manager()
    cookie_usuario = cookie_manager.get(cookie="usuario_coleta")
    
    if cookie_usuario:
        st.session_state['usuario_logado_temp'] = cookie_usuario
        return cookie_usuario

    st.title("🔒 Acesso Restrito - Coleta")
    col1, col2 = st.columns([2,1])
    with col1:
        with st.form("form_login"):
            user_input = st.selectbox("Usuário", ["Selecione..."] + list(usuarios.keys()))
            pass_input = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar", type="primary")
            
            if submitted:
                if user_input != "Selecione..." and pass_input == usuarios[user_input]:
                    st.session_state['usuario_logado_temp'] = user_input
                    try: cookie_manager.set("usuario_coleta", user_input, expires_at=datetime.now() + pd.Timedelta(days=1))
                    except: pass
                    st.rerun()
                else: st.error("Senha incorreta.")
    st.stop()

# --- TELA ADMIN ---
def tela_admin_area():
    st.markdown("## ⚙️ Painel do Administrador")
    aba1, aba2 = st.tabs(["📤 Criar Novo Projeto", "📥 Baixar Relatórios"])
    
    with aba1:
        st.markdown("### 1º Passo: Obter o Modelo")
        st.download_button("📥 Baixar Modelo (.xlsx)", services.gerar_modelo_padrao(), "modelo.xlsx")
        st.divider()
        st.markdown("### 2º Passo: Enviar Arquivo")
        with st.form("form_upload"):
            arquivo = st.file_uploader("Arquivo Excel", type=["xlsx"])
            btn_processar = st.form_submit_button("🚀 Criar Projeto", type="primary")
        
        if btn_processar and arquivo:
            try:
                df = pd.read_excel(arquivo, dtype=str)
                df.columns = [services.remove_accents(str(c).lower().strip().replace(" ","")) for c in df.columns]
                if all(c in df.columns for c in ['site*', 'descricao*', 'ean*', 'quantidadenolote*']):
                    with st.spinner("Processando..."):
                        id_proj, qtd, tam = services.processar_upload_lotes(df, arquivo.name)
                        st.success(f"Criado! ID: {id_proj}. Lotes de: {tam}"); st.balloons()
                else: st.error("Faltam colunas obrigatórias.")
            except Exception as e: st.error(f"Erro: {e}")
    
    with aba2:
        st.markdown("### 📥 Download")
        projetos = services.carregar_projetos_ativos()
        if not projetos.empty:
            proj_dict = {row['nome']: row['id'] for _, row in projetos.iterrows()}
            sel_proj = st.selectbox("Projeto:", list(proj_dict.keys()))
            if st.button("📦 Preparar Download"):
                with st.spinner("Baixando..."):
                    st.download_button("📥 Baixar Excel", services.baixar_projeto_completo(proj_dict[sel_proj]), f"{sel_proj}.xlsx")

# --- TELA PRODUÇÃO ---
def tela_producao(usuario):
    st.title(f"🏭 Área de Coleta | {usuario}")
    projetos = services.carregar_projetos_ativos()
    if projetos.empty: st.info("Sem projetos."); return

    proj_dict = {row['nome']: row['id'] for _, row in projetos.iterrows()}
    nome_proj = st.selectbox("Projeto:", ["Selecione..."] + list(proj_dict.keys()), key="sb_proj_prod")
    if nome_proj == "Selecione...": st.stop()
    id_proj = proj_dict[nome_proj]
    
    df_lotes = services.carregar_lotes_do_projeto(id_proj)
    if df_lotes.empty: st.warning("Sem lotes."); return

    with st.expander("📊 Ver Mapa Geral"):
        st.dataframe(df_lotes[['usuario', 'lote', 'status']], hide_index=True)

    st.divider()

    # Seleção de Lote
    if 'lote_trabalho' not in st.session_state:
        meus = df_lotes[(df_lotes['status'] == 'Em Andamento') & (df_lotes['usuario'] == usuario)]['lote'].unique()
        livres = df_lotes[df_lotes['status'] == 'Livre']['lote'].unique()
        opcoes = [f"Lote {l} (RETOMAR)" for l in sorted(meus)] + [f"Lote {l} (NOVO)" for l in sorted(livres)]
        
        c1, c2 = st.columns([3, 1])
        with c1: escolha = st.selectbox("Escolha:", ["Selecione..."] + opcoes, key="sb_lote_sel")
        with c2:
            if escolha != "Selecione..." and st.button("Acessar", type="primary"):
                num = int(escolha.split()[1])
                if num in livres and not services.reservar_lote(id_proj, num, usuario):
                    st.error("Erro."); time.sleep(1); st.rerun()
                st.session_state.update({'lote_trabalho': num, 'status_trabalho': 'TRABALHANDO', 'hora_inicio_sessao': datetime.now(services.TZ_BRASIL)})
                if 'df_lote_cache' in st.session_state: del st.session_state['df_lote_cache']
                st.rerun()
    else:
        # Modo Trabalho
        num_lote = st.session_state['lote_trabalho']
        
        # Carregamento Inicial para Cache de Sessão
        if 'df_lote_cache' not in st.session_state:
            df_raw = services.carregar_dados_lote(id_proj, num_lote)
            # Lógica de Checkpoint e Busca
            lote_info = df_lotes[df_lotes['lote'] == str(num_lote)]
            checkpoint = lote_info.iloc[0]['checkpoint'] if not lote_info.empty and str(lote_info.iloc[0]['checkpoint']) not in ["nan", ""] else ""
            
            df_raw.insert(0, "MARCADOR", "")
            df_raw['BUSCA_GOOGLE'] = df_raw.apply(lambda x: f"https://www.google.com/search?q={x['ean']}+{x['descricao']}".replace(" ", "+"), axis=1)
            
            if checkpoint:
                mask = df_raw['descricao'].astype(str).str.strip() == checkpoint
                df_raw.loc[mask, 'MARCADOR'] = ">>> PAREI AQUI <<<"
            
            st.session_state['df_lote_cache'] = df_raw
            st.session_state['checkpoint_cache'] = checkpoint

        df_dados = st.session_state['df_lote_cache']
        checkpoint_val = st.session_state.get('checkpoint_cache', "")

        # Callback de salvamento
        def salvar_mudancas():
            if "editor_links" in st.session_state:
                changes = st.session_state["editor_links"].get("edited_rows", {})
                for idx, val in changes.items():
                    if "link" in val:
                        services.salvar_alteracao_individual(id_proj, num_lote, int(idx), val["link"], df_dados)
                        st.session_state['df_lote_cache'].at[int(idx), 'link'] = val["link"]
                if changes: st.toast("Salvo!", icon="💾")

        # Visual Cabeçalho
        if not df_dados.empty:
            ui.render_header_lote(num_lote, df_dados.iloc[0]['site'], df_dados.iloc[0]['cep'], df_dados.iloc[0]['endereco'])
            if checkpoint_val: st.info(f"📍 Parou em: {checkpoint_val}")

        if st.session_state.get('status_trabalho') == 'PAUSADO':
            st.warning(f"⏸️ Lote {num_lote} Pausado")
            if st.button("▶️ RETOMAR"):
                st.session_state['status_trabalho'] = 'TRABALHANDO'
                st.session_state['hora_inicio_sessao'] = datetime.now(services.TZ_BRASIL)
                st.rerun()
        else:
            # Editor
            foco = st.toggle("🎯 Modo Foco")
            cols = ['MARCADOR', 'ean', 'descricao', 'BUSCA_GOOGLE', 'link']
            df_view = df_dados[cols].copy()
            if foco: df_view = df_view[(df_view['link'] == "") | (df_view['link'].isna())]

            st.data_editor(
                df_view,
                key="editor_links",
                on_change=salvar_mudancas,
                column_config={
                    "MARCADOR": st.column_config.TextColumn("Marcador", disabled=True),
                    "ean": st.column_config.TextColumn("EAN", disabled=True),
                    "BUSCA_GOOGLE": st.column_config.LinkColumn("Ajuda", display_text="🔍 Buscar"),
                    "link": st.column_config.LinkColumn("Link", width="large")
                },
                hide_index=True, use_container_width=True, height=600, num_rows="fixed" if not foco else "dynamic"
            )
            
            # Botões Finais
            st.divider()
            c1, c2 = st.columns(2)
            with c1:
                # Selectbox para pausa
                descricoes = df_dados['descricao'].tolist()
                try: idx_def = descricoes.index(checkpoint_val) + 1 if checkpoint_val in descricoes else 0
                except: idx_def = 0
                sel_pausa = st.selectbox("Parar onde?", ["Nada"] + descricoes, index=idx_def, key="sb_pausa")
                
                if st.button("💾 Pausar"):
                    val = sel_pausa if sel_pausa != "Nada" else ""
                    services.salvar_progresso_lote(st.session_state['df_lote_cache'], id_proj, num_lote, False, val)
                    delta = datetime.now(services.TZ_BRASIL) - st.session_state['hora_inicio_sessao']
                    services.salvar_log_tempo(usuario, id_proj, nome_proj, num_lote, delta.total_seconds(), "Pausa", len(df_dados), 0)
                    st.session_state['status_trabalho'] = 'PAUSADO'
                    del st.session_state['df_lote_cache']
                    st.rerun()
            with c2:
                if st.button("✅ Entregar Lote", type="primary"):
                    services.salvar_progresso_lote(st.session_state['df_lote_cache'], id_proj, num_lote, True)
                    delta = datetime.now(services.TZ_BRASIL) - st.session_state['hora_inicio_sessao']
                    services.salvar_log_tempo(usuario, id_proj, nome_proj, num_lote, delta.total_seconds(), "Fim", len(df_dados), 0)
                    for k in ['lote_trabalho', 'hora_inicio_sessao', 'status_trabalho', 'df_lote_cache']:
                        if k in st.session_state: del st.session_state[k]
                    st.balloons(); time.sleep(2); st.rerun()