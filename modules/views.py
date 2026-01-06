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

# --- FRAGMENTO DA TABELA (COM SCROLL FIXO E PERFORMANCE) ---
@st.fragment
def fragmento_tabela(id_p, lote, user, nome_p):
    if 'df_cache' not in st.session_state:
        st.error("⚠️ Erro de estado. Por favor, aperte F5.")
        return
    
    # 1. MESTRE (O Banco de Dados na Memória)
    df_ref = st.session_state['df_cache']

    # 2. CALLBACK DE SALVAMENTO (O Cérebro)
    def callback_salvar():
        # Pega as alterações enviadas pelo editor
        # O Streamlit retorna um dicionário: {"indice_original": {"coluna": "valor"}}
        changes = st.session_state["editor_links"].get("edited_rows", {})
        
        if not changes: return

        lista_para_salvar = []
        
        for idx_str, val in changes.items():
            idx = int(idx_str) # Esse é o índice ORIGINAL da linha (não importa se filtrou)
            
            if "link" in val:
                novo_link = val["link"]
                
                # A. Atualiza a memória MESTRE imediatamente
                df_ref.at[idx, 'link'] = novo_link
                
                # B. Prepara o pacote para o Google Sheets
                # Usamos a coluna oculta _row_index para garantir que vai na linha certa do Excel
                linha_excel = int(df_ref.iloc[idx]['_row_index'])
                
                lista_para_salvar.append({
                    'indice_excel': linha_excel,
                    'link': novo_link
                })
                
                # C. Feedback Visual Instantâneo
                if novo_link and str(novo_link).strip() != "":
                    st.toast(f"✅ Item salvo!", icon="⚡")

        # D. Envia para o Google em Lote (Sem travar a tela)
        if lista_para_salvar:
            # Chama o serviço de lote (que já está blindado com cache e retry)
            services.salvar_lote_links(id_p, lote, lista_para_salvar)

    # 3. PREPARAÇÃO DA VISUALIZAÇÃO (A Fila)
    # Filtramos apenas o que NÃO tem link.
    # O .copy() é crucial para o Streamlit entender que é uma nova renderização limpa.
    mask_pendentes = (df_ref['link'] == "") | (df_ref['link'].isna())
    df_view = df_ref[mask_pendentes].copy()

    # Criação da coluna de busca (caso não exista)
    if 'BUSCA_GOOGLE' not in df_view.columns:
        df_view['BUSCA_GOOGLE'] = df_view.apply(lambda x: f"https://www.google.com/search?q={x['ean']}", axis=1)

    # Métricas de Progresso
    total = len(df_ref)
    restantes = len(df_view)
    feitos = total - restantes
    progresso = int((feitos / total) * 100) if total > 0 else 0

    # 4. RENDERIZAÇÃO DA TELA
    c_metrics, c_entrega = st.columns([3, 1])
    
    with c_metrics:
        st.markdown(f"### 🔨 Fila de Trabalho: **{restantes}** itens restantes")
        st.progress(progresso, text=f"Progresso: {feitos}/{total} concluídos")

    # SE ACABOU O TRABALHO
    if df_view.empty:
        st.success("🎉 PARABÉNS! Lote finalizado.")
        st.markdown("Confira se está tudo certo e clique em **Entregar Lote** ao lado.")
        st.balloons()
    
    # SE AINDA TEM TRABALHO
    else:
        # Define as colunas exatas
        cols_config = {
            "MARCADOR": st.column_config.TextColumn("Status", disabled=True, width="small"),
            "ean": st.column_config.TextColumn("EAN", disabled=True, width="medium"),
            "descricao": st.column_config.TextColumn("Descrição do Produto", disabled=True),
            "BUSCA_GOOGLE": st.column_config.LinkColumn("Ajuda", display_text="🔍 Buscar no Google", width="small"),
            "link": st.column_config.LinkColumn(
                "Cole o Link Aqui 👇", 
                validate="^https?://", 
                width="large",
                help="Cole o link e aperte Enter. A linha sumirá e será salva."
            )
        }
        
        cols_ordem = ['ean', 'descricao', 'BUSCA_GOOGLE', 'link']
        if 'MARCADOR' in df_view.columns: cols_ordem.insert(0, 'MARCADOR')

        # TABELA EDITÁVEL
        st.data_editor(
            df_view,                  # Mostra apenas os pendentes
            key="editor_links",       # Chave única
            on_change=callback_salvar,# Salva assim que edita
            column_config=cols_config,
            column_order=cols_ordem,
            hide_index=True,          # Esconde o índice numérico feio
            use_container_width=True, # Ocupa a largura toda
            height=500,               # Altura fixa para conforto
            num_rows="fixed"          # Impede adicionar/remover linhas
        )

    # 5. RODAPÉ (Pausa e Entrega)
    st.divider()
    c1, c2 = st.columns(2)
    
    with c1:
        # Lógica de Pausa Inteligente
        # Mostra apenas as descrições que ainda faltam para facilitar a escolha
        opcoes_pausa = ["(Não pausar agora)"] + df_view['descricao'].tolist()[:10] # Mostra só os próximos 10
        
        sel_pausa = st.selectbox("Precisa pausar? Marque o próximo item a fazer:", opcoes_pausa)
        
        if st.button("💾 Salvar Checkpoint e Sair"):
            check = sel_pausa if sel_pausa != "(Não pausar agora)" else ""
            with st.spinner("Salvando posição..."):
                services.salvar_progresso_lote(df_ref, id_p, lote, False, check)
                tempo = (datetime.now(services.TZ_BRASIL) - st.session_state['h_ini']).total_seconds()
                services.salvar_log_tempo(user, id_p, nome_p, lote, tempo, "Pausa", total, feitos)
                
                # Limpa sessão
                st.session_state['status'] = 'PAUSADO'
                del st.session_state['df_cache']
                if 'saved_indices' in st.session_state: del st.session_state['saved_indices']
                st.rerun()

    with c2:
        # Botão de Entrega (Só habilita se acabar ou se o usuário quiser forçar)
        label_btn = "✅ Entregar Lote Completo" if df_view.empty else "⚠️ Entregar Lote Incompleto"
        type_btn = "primary" if df_view.empty else "secondary"
        
        if st.button(label_btn, type=type_btn):
            if not df_view.empty:
                st.warning("Tem certeza? Ainda existem itens sem link.")
                time.sleep(1)
            
            with st.spinner("Finalizando e sincronizando..."):
                # Garante um último salvamento geral
                services.salvar_progresso_lote(df_ref, id_p, lote, True)
                tempo = (datetime.now(services.TZ_BRASIL) - st.session_state['h_ini']).total_seconds()
                services.salvar_log_tempo(user, id_p, nome_p, lote, tempo, "Fim", total, feitos)
                
                # Limpa tudo
                for k in ['lote_ativo', 'h_ini', 'status', 'df_cache', 'saved_indices']: 
                    if k in st.session_state: del st.session_state[k]
                
                st.balloons()
                time.sleep(1)
                st.rerun()

    with c2:
        # Botão de entregar só habilita se tudo estiver feito (opcional, mas recomendado)
        # Se quiser permitir entrega parcial, remova o 'disabled'
        pode_entregar = df_show.empty 
        if st.button("✅ Entregar Lote", type="primary", disabled=not pode_entregar, help="Termine todos os itens para entregar"):
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
            
            # CRIAÇÃO DA COLUNA DE BUSCA (AQUI, UMA VEZ SÓ)
            if 'BUSCA_GOOGLE' not in df.columns:
                df['BUSCA_GOOGLE'] = df.apply(lambda x: f"https://www.google.com/search?q={x['ean']}", axis=1)

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