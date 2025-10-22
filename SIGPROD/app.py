import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_file, render_template, send_from_directory
from flask_cors import CORS
import pandas as pd
import io
import re
from datetime import datetime
from sqlalchemy import create_engine, text

# Load environment variables from .env at project root
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

app = Flask(__name__)
CORS(app)

# --- Configuração do Banco de Dados com SQLAlchemy ---
DB_USER = os.environ.get("DB_USER", "your_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "your_password")
DB_HOST = os.environ.get("DB_HOST", "192.168.0.253")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "your_dbname")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public").strip()

# URL de conexão para o SQLAlchemy
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# O 'engine' gerencia as conexões com o banco de dados de forma eficiente
engine = create_engine(DATABASE_URL)

# --- Funções Utilitárias ---
def fq(table_name: str) -> str:
    if DB_SCHEMA:
        return f'{DB_SCHEMA}.{table_name}'
    return table_name

def fetch_data_from_db(query, params=None):
    """Executa a consulta usando o engine do SQLAlchemy, eliminando o warning."""
    try:
        # Pandas trabalha diretamente com o engine, de forma otimizada
        df = pd.read_sql_query(sql=query, con=engine, params=params)
        return df, None
    except Exception as e:
        print(f"Erro ao executar a consulta com SQLAlchemy: {e}")
        return None, f"Erro ao executar a consulta: {e}"

def table_exists(table_name: str) -> bool:
    """Verifica se uma tabela existe usando o engine do SQLAlchemy."""
    try:
        with engine.connect() as connection:
            sql = text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_name = :table
                )
            """)
            result = connection.execute(sql, {"schema": (DB_SCHEMA or 'public'), "table": table_name})
            return bool(result.scalar_one())
    except Exception as e:
        print(f"Falha ao checar existencia de tabela {DB_SCHEMA}.{table_name}: {e}")
        return False

_pasfase_cols_cache = {}
def _pasfase_columns():
    """Busca os nomes das colunas da tabela pasfase usando SQLAlchemy."""
    if 'cols' in _pasfase_cols_cache:
        return _pasfase_cols_cache['cols']
    
    try:
        with engine.connect() as connection:
            sql = text("SELECT column_name FROM information_schema.columns WHERE table_schema = :schema AND table_name = 'pasfase'")
            result = connection.execute(sql, {"schema": (DB_SCHEMA or 'public')})
            cols = {row[0].lower() for row in result}
            ordem_col = next((c for c in ['ordem', 'pasordem', 'ordnum'] if c in cols), 'ordem')
            qtd_col = next((c for c in ['pasquanti', 'pasquant', 'pasqtd'] if c in cols), 'pasquanti')
            _pasfase_cols_cache['cols'] = (ordem_col, qtd_col)
            return ordem_col, qtd_col
    except Exception:
        return 'ordem', 'pasquanti'

# --- Lógica de Processamento de Dados ---
def _parse_tapecaria_dates(text: str):
    """Função de parsing de data específica para tapeçaria."""
    if not text: return None, None
    try:
        def to_iso(s: str):
            parts = s.split('/')
            if len(parts) != 3: return None
            dd, mm, yy = parts
            if len(yy) == 2: yy = '20' + yy
            return f"{yy}-{mm}-{dd}"

        pattern_tapecaria = r"TAPECARIA\(136\):\s*(\d{2}/\d{2}/\d{2,4})\s*-\s*(\d{2}/\d{2}/\d{2,4})"
        m_tapecaria = re.search(pattern_tapecaria, str(text), flags=re.IGNORECASE)
        if m_tapecaria:
            return to_iso(m_tapecaria.group(1)), to_iso(m_tapecaria.group(2))
        
        pattern_tapecaria_single = r"TAPECARIA\(136\):\s*(\d{2}/\d{2}/\d{2,4})"
        m_tapecaria_single = re.search(pattern_tapecaria_single, str(text), flags=re.IGNORECASE)
        if m_tapecaria_single:
            iso_date = to_iso(m_tapecaria_single.group(1))
            return iso_date, iso_date
        
        return None, None
    except Exception:
        return None, None

def _parse_phase_dates(text: str, phase_name: str):
    if not text or not phase_name: return None, None
    try:
        # Usamos `\b` para garantir que estamos pegando a palavra exata (ex: "MONTAGEM" e não "MONTAGEMSEP")
        pattern = rf"\b{re.escape(phase_name)}\b:\s*(\d{{2}}/\d{{2}}/\d{{2,4}})\s*-\s*(\d{{2}}/\d{{2}}/\d{{2,4}})"
        m = re.search(pattern, str(text), flags=re.IGNORECASE)
        if not m: return None, None
        def to_iso(s: str):
            parts = s.split('/')
            if len(parts) != 3: return None
            dd, mm, yy = parts
            if len(yy) == 2: yy = '20' + yy
            return f"{yy}-{mm}-{dd}"
        return to_iso(m.group(1)), to_iso(m.group(2))
    except Exception:
        return None, None

def process_data(df: pd.DataFrame, fase: int) -> pd.DataFrame:
    if df.empty: return df

    if fase == 136:
        if 'lote_trans' in df.columns:
            dates = df['lote_trans'].fillna('').apply(_parse_tapecaria_dates)
            df[['data_inicio_prevista', 'data_fim_prevista']] = pd.DataFrame(dates.tolist(), index=df.index)
            df['corte_dtini'] = pd.to_datetime(df['data_inicio_prevista'], errors='coerce')
            df['orddtprev'] = pd.to_datetime(df['data_fim_prevista'], errors='coerce')
        else:
            df['corte_dtini'] = pd.NaT
            df['orddtprev'] = pd.NaT

        today = pd.to_datetime('today').normalize()
        df['status'] = 'futuro'
        on_time_mask = (df['corte_dtini'].notna()) & (df['orddtprev'].notna()) & (df['corte_dtini'] <= today) & (df['orddtprev'] >= today)
        df.loc[on_time_mask, 'status'] = 'em_dia'
        delayed_mask = (df['orddtprev'].notna()) & (df['orddtprev'] < today)
        df.loc[delayed_mask, 'status'] = 'atrasado'
        
        df_atrasado = df[df['status'] == 'atrasado']
        df_nao_atrasado = df[df['status'] != 'atrasado'].copy()
        df_em_dia = pd.DataFrame()

        if not df_nao_atrasado.empty:
            df_ativas_ou_futuras_proximas = df_nao_atrasado[df_nao_atrasado['corte_dtini'] <= today]
            
            if not df_ativas_ou_futuras_proximas.empty:
                 df_em_dia = df_ativas_ou_futuras_proximas
            else:
                proxima_data_inicio = df_nao_atrasado['corte_dtini'].min()
                if pd.notna(proxima_data_inicio):
                    df_em_dia = df_nao_atrasado[df_nao_atrasado['corte_dtini'] == proxima_data_inicio]

        if not df_atrasado.empty or not df_em_dia.empty:
            return pd.concat([df_atrasado, df_em_dia])
        else:
            return pd.DataFrame(columns=df.columns)

    # O nome da fase para o parsing das datas pode ser diferente do nome do monitor.
    phase_parse_key_map = {
        5: 'CORTE', 
        10: 'PRENSA', 
        15: 'USINAGEM', 
        25: 'MONTAGEM',
        30: 'MONTAGEM',
        35: 'ACABAMENTO',
        998: 'MONTAGEMSEP', # Chave para Saida para Montagem
        999: 'PREACABAMENT'  # Chave EXATA para o monitor Saída para Pintura
    }
    phase_name_for_parsing = phase_parse_key_map.get(fase, 'CORTE')

    # 1. Parse de Datas
    if 'lote_trans' in df.columns:
        dates = df['lote_trans'].fillna('').apply(lambda x: _parse_phase_dates(x, phase_name_for_parsing))
        df[['data_inicio_prevista', 'data_fim_prevista']] = pd.DataFrame(dates.tolist(), index=df.index)
        df['corte_dtini'] = pd.to_datetime(df['data_inicio_prevista'], errors='coerce')
        df['orddtprev'] = pd.to_datetime(df['data_fim_prevista'], errors='coerce')
    else:
        df['corte_dtini'] = pd.NaT
        df['orddtprev'] = pd.NaT

    # 2. Definir Status
    today = pd.to_datetime('today').normalize()
    df['status'] = 'futuro'
    on_time_mask = (df['corte_dtini'].notna()) & (df['orddtprev'].notna()) & (df['corte_dtini'] <= today) & (df['orddtprev'] >= today)
    df.loc[on_time_mask, 'status'] = 'em_dia'
    delayed_mask = (df['orddtprev'].notna()) & (df['orddtprev'] < today)
    df.loc[delayed_mask, 'status'] = 'atrasado'
    
    # 3. Aplicar Lógica de Sequenciamento apenas para fase 15
    if fase == 15 and 'lote_descricao' in df.columns and df['lote_descricao'].notna().any():
        lots = df.groupby('lote_descricao').agg(
            min_start_date=('corte_dtini', 'min'),
            status=('status', lambda s: 'atrasado' if 'atrasado' in s.values else ('em_dia' if 'em_dia' in s.values else 'futuro'))
        ).dropna(subset=['min_start_date']).sort_values('min_start_date')

        lots_to_display = []
        has_ontime_op = False
        for lote, data in lots.iterrows():
            if data['status'] == 'atrasado':
                lots_to_display.append(lote)
            elif data['status'] == 'em_dia' and not has_ontime_op:
                lots_to_display.append(lote)
                has_ontime_op = True
                break
        
        return df[df['lote_descricao'].isin(lots_to_display)]
    
    # Para a pintura, retorna tudo para o frontend fazer a lógica de exibição
    if fase == 35:
        return df
    
    # Para outras fases, apenas retorna o que está em dia ou atrasado
    return df[df['status'].isin(['atrasado', 'em_dia'])]

# --- Rotas de Renderização ---
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/monitor_corte')
def monitor_corte_page():
    return render_template('corte.html')

@app.route('/monitor_prensa')
def monitor_prensa_page():
    return render_template('prensa.html')

@app.route('/monitor_usinagem')
def monitor_usinagem_page():
    return render_template('usinagem.html')

@app.route('/monitor_saida_montagem')
def monitor_saida_montagem_page():
    return render_template('saida_montagem.html')

@app.route('/monitor_macico')
def monitor_macico_page():
    return render_template('macico.html')

@app.route('/monitor_chapa')
def monitor_chapa_page():
    return render_template('chapa.html')

@app.route('/monitor_saida_pintura')
def monitor_saida_pintura_page():
    return render_template('saida_pintura.html')

@app.route('/monitor_pintura')
def monitor_pintura_page():
    return render_template('pintura.html')

@app.route('/monitor_tapecaria')
def monitor_tapecaria_page():
    return render_template('tapecaria.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)
    
# --- Rotas da API ---
def get_base_query(lot_table, ord_col, qtd_col, fase: int):
    # Define o filtro de LOTE (OP)
    if fase in [25, 30, 35, 998, 999, 136]: 
        op_filter = "(l.lotdes ILIKE '%%Petra%%' OR l.lotdes ILIKE '%%Solare%%' OR l.lotdes ILIKE '%%Garland%%')"
    else:
        op_filter = "l.lotdes ILIKE '%%OSSO%%' AND l.lotdes NOT ILIKE '%%AVULSO%%'"

    # --- Lógica para Saída para Montagem (998) ---
    if fase == 998:
        if not all(table_exists(tbl) for tbl in ['reqordem', 'toqmovi']): return "SELECT 1 WHERE 1=0"
        return f"""
            WITH
            requisicoes_base AS (
                SELECT r.reqord, r.reqproduto, r.rqoquanti, r.reqnumero, l.lotdes AS lote_descricao
                FROM {fq('reqordem')} r
                JOIN {fq('ordem')} o ON o.ordem = r.reqord
                JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
                JOIN {fq('produto')} p ON p.produto = r.reqproduto
                WHERE r.rqoquanti > 0 AND r.reqfase = 17 AND {op_filter} AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
                  AND p.profantasm = 'N'
                  AND p.proorigem = 'F'
                  AND p.pronome NOT ILIKE '%%CANTONEIRA%%' 
                  AND p.pronome NOT ILIKE '%%(ALU)%%' 
                  AND p.pronome NOT ILIKE '%%(FERRO)%%'
            ),
            agregado_req AS (
                SELECT
                    reqord, reqproduto, lote_descricao,
                    TRIM(CAST(reqord AS TEXT)) as reqord_key,
                    TRIM(reqproduto) as reqproduto_key,
                    SUM(rqoquanti) as quanti_req, MAX(reqnumero) as reqnumero
                FROM requisicoes_base GROUP BY reqord, reqproduto, lote_descricao
            ),
            agregado_historico AS (
                SELECT lote_descricao, SUM(rqoquanti) as total_qty_lote FROM requisicoes_base GROUP BY lote_descricao
            ),
            total_deb AS (
                SELECT
                    TRIM(CAST(m.priordem AS TEXT)) as priordem_key,
                    TRIM(m.priproduto) as priproduto_key,
                    SUM(m.priquanti) as quanti_deb
                FROM {fq('toqmovi')} m
                WHERE EXISTS (SELECT 1 FROM requisicoes_base rb WHERE TRIM(CAST(rb.reqord AS TEXT)) = TRIM(CAST(m.priordem AS TEXT)) AND TRIM(rb.reqproduto) = TRIM(m.priproduto))
                AND m.pritransac = '14' AND (m.priobserv IS NULL OR m.priobserv = '') AND m.pridata >= '2025-01-01'
                GROUP BY 1, 2
            ),
            dados_saldo AS (
                SELECT ar.reqord AS ordem, ar.reqproduto AS produto, ar.lote_descricao, ar.reqnumero,
                    GREATEST(ar.quanti_req - COALESCE(deb.quanti_deb, 0), 0) AS saldo_pendente
                FROM agregado_req ar
                LEFT JOIN total_deb deb ON deb.priordem_key = ar.reqord_key AND deb.priproduto_key = ar.reqproduto_key
            )
            SELECT ds.ordem, ds.produto, p.pronome AS descricao, ds.saldo_pendente, ds.reqnumero, ds.lote_descricao,
                ah.total_qty_lote AS total_historico_lote, o.ordquanti, o.orddtence,
                l.lottrans AS lote_trans, l.lotdtini, l.lotdtpre,
                0 as devolucao_saldo
            FROM dados_saldo ds
            JOIN agregado_historico ah ON ds.lote_descricao = ah.lote_descricao
            JOIN {fq('ordem')} o ON o.ordem = ds.ordem
            JOIN {fq('produto')} p ON p.produto = ds.produto
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            WHERE ds.saldo_pendente > 0
        """

    # --- Lógica para Saída para Pintura (999) ---
    elif fase == 999:
        if not all(table_exists(tbl) for tbl in ['reqordem', 'toqmovi', 'processo']): return "SELECT 1 WHERE 1=0"
        return f"""
            WITH
            requisicoes_base AS (
                SELECT r.reqord, r.reqproduto, r.rqoquanti, r.reqnumero, l.lotdes AS lote_descricao
                FROM {fq('reqordem')} r
                JOIN {fq('ordem')} o ON o.ordem = r.reqord
                JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
                WHERE EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = r.reqproduto AND pr.fase IN (25, 30))
                AND r.reqproduto ILIKE 'OSS%%' AND r.rqoquanti > 0 AND {op_filter} AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
            ),
            agregado_req AS (
                SELECT reqord, reqproduto, lote_descricao, SUM(rqoquanti) as quanti_req, MAX(reqnumero) as reqnumero
                FROM requisicoes_base GROUP BY reqord, reqproduto, lote_descricao
            ),
            agregado_historico AS (
                SELECT lote_descricao, SUM(rqoquanti) as total_qty_lote FROM requisicoes_base GROUP BY lote_descricao
            ),
            total_deb AS (
                SELECT m.priordem, m.priproduto, SUM(m.priquanti) as quanti_deb
                FROM {fq('toqmovi')} m
                WHERE EXISTS (SELECT 1 FROM requisicoes_base rb WHERE TRIM(CAST(rb.reqord AS TEXT)) = TRIM(CAST(m.priordem AS TEXT)) AND TRIM(rb.reqproduto) = TRIM(m.priproduto))
                AND m.pritransac = '14' AND (m.priobserv IS NULL OR m.priobserv = '') AND m.pridata >= '2025-01-01'
                GROUP BY m.priordem, m.priproduto
            ),
            dados_saldo AS (
                SELECT ar.reqord AS ordem, ar.reqproduto AS produto, ar.lote_descricao, ar.reqnumero,
                    GREATEST(ar.quanti_req - COALESCE(deb.quanti_deb, 0), 0) AS saldo_pendente
                FROM agregado_req ar
                LEFT JOIN total_deb deb ON TRIM(CAST(deb.priordem AS TEXT)) = TRIM(CAST(ar.reqord AS TEXT)) AND TRIM(deb.priproduto) = TRIM(ar.reqproduto)
            )
            SELECT ds.ordem, ds.produto, p.pronome AS descricao, ds.saldo_pendente, ds.reqnumero, ds.lote_descricao,
                ah.total_qty_lote AS total_historico_lote, o.ordquanti, o.orddtence,
                l.lottrans AS lote_trans, l.lotdtini, l.lotdtpre,
                0 as devolucao_saldo
            FROM dados_saldo ds
            JOIN agregado_historico ah ON ds.lote_descricao = ah.lote_descricao
            JOIN {fq('ordem')} o ON o.ordem = ds.ordem
            JOIN {fq('produto')} p ON p.produto = ds.produto
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            WHERE ds.saldo_pendente > 0
        """
        
    elif fase == 136:
        if not all(table_exists(tbl) for tbl in ['ordem', 'processo', 'planilha', 'produto', lot_table]):
            return "SELECT 1 WHERE 1=0"
        return f"""
            WITH 
            ordens_com_fase AS (
                SELECT o.ordem
                FROM {fq('ordem')} o
                JOIN {fq('processo')} pr ON o.ordproduto = pr.produto
                JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
                WHERE pr.prccodig = '136' AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
            ),
            quantidades_planilhadas AS (
                SELECT 
                    CAST(plaordem AS TEXT) as ordem, 
                    SUM(COALESCE(CAST(plaquant AS NUMERIC), 0)) as qtd_planilhada
                FROM {fq('planilha')} 
                WHERE plaopera = '136' 
                GROUP BY plaordem
            ),
            total_historico_por_lote AS (
                SELECT l.lotdes, SUM(o.ordquanti) as total_qty_lote
                FROM {fq('ordem')} o JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
                WHERE EXISTS (SELECT 1 FROM ordens_com_fase ocf WHERE ocf.ordem = o.ordem)
                GROUP BY l.lotdes
            )
            SELECT
                o.ordem, 
                o.ordproduto AS produto, 
                p.pronome AS descricao,
                GREATEST(o.ordquanti - COALESCE(qp.qtd_planilhada, 0), 0) AS saldo_pendente,
                o.ordquanti, 
                o.orddtence, 
                l.lotdes AS lote_descricao,
                l.lottrans AS lote_trans, 
                l.lotdtini, 
                l.lotdtpre,
                th.total_qty_lote as total_historico_lote,
                0 as devolucao_saldo
            FROM {fq('ordem')} o
            JOIN ordens_com_fase ocf ON o.ordem = ocf.ordem
            JOIN {fq('produto')} p ON p.produto = o.ordproduto
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            LEFT JOIN quantidades_planilhadas qp ON CAST(o.ordem AS TEXT) = qp.ordem
            LEFT JOIN total_historico_por_lote th ON th.lotdes = l.lotdes
            WHERE GREATEST(o.ordquanti - COALESCE(qp.qtd_planilhada, 0), 0) > 0
        """

    # --- Lógica Padrão para os outros monitores ---
    devolucoes_cte = ""
    perdas_cte = ""
    saldo_pendente_calc = "GREATEST(o.ordquanti - COALESCE(q.qtd, 0), 0)"
    devolucao_saldo_select = "0 as devolucao_saldo,"
    devolucao_join = ""
    perdas_join = ""
    ordem_status_filter = "o.orddtence = DATE '0001-01-01'"
    qtd_fase_source = f"""
        SELECT CAST({ord_col} AS TEXT) AS ordem, SUM(COALESCE({qtd_col}, 0)) AS qtd
        FROM {fq('pasfase')} WHERE fase = %(fase)s GROUP BY {ord_col}
    """

    if fase == 15:
        perdas_cte = f""",
        qtd_perdida AS (
            SELECT CAST(perofscod AS TEXT) as ordem, SUM(COALESCE(perqtdper, 0)) as qtd_perdida
            FROM {fq('perdas')}
            GROUP BY perofscod
        )
        """
        saldo_pendente_calc = "GREATEST(o.ordquanti - (COALESCE(q.qtd, 0) + COALESCE(pds.qtd_perdida, 0)), 0)"
        perdas_join = "LEFT JOIN qtd_perdida pds ON CAST(o.ordem AS TEXT) = pds.ordem"

    if fase in [25, 30]:
        devolucoes_cte = f""",
        devolucoes_saldo AS (
            SELECT
                o.lotcod,
                TRIM(m.priproduto) as produto_key,
                SUM(CASE WHEN m.pritransac = '4' THEN m.priquanti ELSE -m.priquanti END) as saldo_devolucao
            FROM {fq('toqmovi')} m
            JOIN {fq('ordem')} o ON TRIM(CAST(o.ordem AS TEXT)) = TRIM(CAST(m.priordem AS TEXT))
            WHERE m.priobserv ILIKE '%%*d:%%'
              AND m.pridata >= '2025-01-01'
              AND m.pritransac IN ('4', '14')
            GROUP BY 1, 2
        )
        """
        
        # Para a fase 25 (Maciço), a fonte de produção é a toqmovi com transação '3'
        if fase == 25:
            qtd_fase_source = f"""
                SELECT TRIM(CAST(m.priordem AS TEXT)) as ordem, 
                       TRIM(m.priproduto) as produto, 
                       SUM(COALESCE(m.priquanti, 0)) as qtd
                FROM {fq('toqmovi')} m
                WHERE m.pritransac = '3' -- Baixa de produção da OP (parcial) para Maciço
                AND (m.priobserv IS NULL OR m.priobserv NOT ILIKE '%%*d:%%')
                GROUP BY 1, 2
            """
            devolucao_join = f"""
                LEFT JOIN qtd_fase q ON CAST(o.ordem AS TEXT) = q.ordem AND TRIM(o.ordproduto) = q.produto
                LEFT JOIN devolucoes_saldo ds ON ds.lotcod = o.lotcod AND ds.produto_key = TRIM(o.ordproduto)
            """
        else: # Para a fase 30 (Chapa), mantém a fonte original (pasfase)
             devolucao_join = f"""
                LEFT JOIN qtd_fase q ON CAST(o.ordem AS TEXT) = q.ordem
                LEFT JOIN devolucoes_saldo ds ON ds.lotcod = o.lotcod AND ds.produto_key = TRIM(o.ordproduto)
            """

        saldo_pendente_calc = "GREATEST(o.ordquanti - COALESCE(q.qtd, 0) + COALESCE(ds.saldo_devolucao, 0), 0)"
        devolucao_saldo_select = "COALESCE(ds.saldo_devolucao, 0) as devolucao_saldo,"
        ordem_status_filter = "(o.orddtence = DATE '0001-01-01' OR COALESCE(ds.saldo_devolucao, 0) > 0)"


    if fase == 35 and table_exists('planilha'):
        # Para a pintura, uma ordem é considerada pendente até que sua produção
        # seja totalmente registrada na planilha, independentemente de a ordem estar encerrada.
        ordem_status_filter = "1=1"  # Ignora o status de encerramento da ordem (orddtence)
        qtd_fase_source = f"""
            SELECT CAST(plaordem AS TEXT) AS ordem, SUM(COALESCE(CAST(plaquant AS NUMERIC), 0)) AS qtd
            FROM {fq('planilha')} WHERE plafase = %(fase)s GROUP BY plaordem
        """
    
    # Para a fase 25 e 30, a junção já está no `devolucao_join`
    base_join = "" if fase in [25, 30] else "LEFT JOIN qtd_fase q ON CAST(o.ordem AS TEXT) = q.ordem"

    return f"""
        WITH qtd_fase AS ({qtd_fase_source})
        {devolucoes_cte}
        {perdas_cte},
        total_historico_por_lote AS (
            SELECT l.lotdes, SUM(o.ordquanti) as total_qty_lote
            FROM {fq('ordem')} o JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
            WHERE {op_filter}
            AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
            AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = o.ordproduto AND pr.fase = %(fase)s)
            GROUP BY l.lotdes
        ),
        dados_filtrados AS (
            SELECT
                o.ordem, o.ordproduto AS produto, p.pronome AS descricao,
                {saldo_pendente_calc} AS saldo_pendente,
                {devolucao_saldo_select}
                o.ordquanti, o.orddtence, l.lotdes AS lote_descricao,
                l.lottrans AS lote_trans, l.lotdtini, l.lotdtpre
            FROM {fq('ordem')} o
            JOIN {fq('produto')} p ON p.produto = o.ordproduto
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            {base_join}
            {devolucao_join}
            {perdas_join}
            WHERE {ordem_status_filter}
              AND {op_filter}
              AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
              AND ({saldo_pendente_calc}) > 0
              AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = o.ordproduto AND pr.fase = %(fase)s)
        )
        SELECT df.*, th.total_qty_lote as total_historico_lote
        FROM dados_filtrados df
        LEFT JOIN total_historico_por_lote th ON th.lotdes = df.lote_descricao
    """

@app.route('/api/data', methods=['GET'])
def get_production_data():
    fase = request.args.get('fase', default=5, type=int)
    lot_table = 'loteprod' if table_exists('loteprod') else 'lotprod'
    if not table_exists(lot_table): return jsonify({"error": f"Tabela de lote '{lot_table}' não encontrada"}), 500
    
    ord_col, qtd_col = _pasfase_columns()

    query = get_base_query(lot_table, ord_col, qtd_col, fase)
    df, error = fetch_data_from_db(query, params={'fase': fase})
    if error: return jsonify({"error": error}), 500
    
    if df is None or df.empty:
        return jsonify({"is_grouped": False, "data": []})

    df_processed = process_data(df.copy(), fase)

    # Para os monitores que agrupam OPs (Maciço, Chapa, Pintura, Saída Pintura, Saída Montagem)
    if fase in [25, 30, 35, 998, 999, 136] and not df_processed.empty:
        df_details = df_processed.copy()
        
        df_details['op_group'] = df_details['lote_descricao'].str.extract(r'((?:OP|O\.P\.?)\s?\d+/\d+)', expand=False).fillna(df_details['lote_descricao'])
        
        agg_rules = {
            'saldo_pendente': ('saldo_pendente', 'sum'),
            'corte_dtini': ('corte_dtini', 'min'),
            'orddtprev': ('orddtprev', 'min'),
        }
        if 'devolucao_saldo' in df_details.columns:
            agg_rules['devolucao_saldo'] = ('devolucao_saldo', 'sum')

        df_summary = df_details.groupby('op_group').agg(**agg_rules).reset_index()

        sub_op_totals = df_details.drop_duplicates(subset=['lote_descricao'])
        total_historico_map = sub_op_totals.groupby('op_group')['total_historico_lote'].sum()
        
        df_summary['total_historico_lote'] = df_summary['op_group'].map(total_historico_map)
        df_summary = df_summary.rename(columns={'op_group': 'lote_descricao'})
        
        today = pd.to_datetime('today').normalize()
        df_summary['status'] = 'futuro'
        on_time_mask = (df_summary['corte_dtini'].notna()) & (df_summary['orddtprev'].notna()) & (df_summary['corte_dtini'] <= today) & (df_summary['orddtprev'] >= today)
        df_summary.loc[on_time_mask, 'status'] = 'em_dia'
        
        delayed_mask = (df_summary['orddtprev'].notna()) & (df_summary['orddtprev'] < today)
        df_summary.loc[delayed_mask, 'status'] = 'atrasado'
        
        for df_to_format in [df_summary, df_details]:
            if not df_to_format.empty:
                sort_cols = ['corte_dtini']
                if 'ordem' in df_to_format.columns: sort_cols.append('ordem')
                else: sort_cols.append('lote_descricao')
                
                df_to_format.sort_values(by=sort_cols, na_position='last', inplace=True)
                date_cols = [col for col in ['orddtprev', 'orddtence', 'lotdtini', 'lotdtpre', 'corte_dtini', 'data_inicio_prevista', 'data_fim_prevista'] if col in df_to_format.columns]
                for col in date_cols:
                    if pd.api.types.is_datetime64_any_dtype(df_to_format[col]):
                        df_to_format[col] = df_to_format[col].dt.strftime('%Y-%m-%d')
        
        return jsonify({
            "is_grouped": True,
            "summary": df_summary.fillna('').to_dict('records'),
            "details": df_details.fillna('').to_dict('records')
        })

    # Para os outros monitores, a estrutura de dados continua a mesma
    if not df_processed.empty:
        df_processed.sort_values(by=['corte_dtini', 'ordem'], na_position='last', inplace=True)
        date_cols = [col for col in ['orddtprev', 'orddtence', 'lotdtini', 'lotdtpre', 'corte_dtini', 'data_inicio_prevista', 'data_fim_prevista'] if col in df_processed.columns]
        for col in date_cols:
            if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
                df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d')

    return jsonify({
        "is_grouped": False,
        "data": df_processed.fillna('').to_dict('records')
    })


@app.route('/api/completed', methods=['GET'])
def get_completed_data():
    fase = request.args.get('fase', default=5, type=int)
    lotes_param = request.args.get('lotes')
    lot_table = 'loteprod' if table_exists('loteprod') else 'lotprod'
    if not table_exists(lot_table): return jsonify({"error": f"Tabela de lote '{lot_table}' não encontrada"}), 500
    
    params = {'fase': fase}
    lote_filter_clause = ""
    if lotes_param:
        lotes_list = [lote.strip() for lote in lotes_param.split(',') if lote.strip()]
        if lotes_list:
            lote_filter_clause = "AND l.lotdes = ANY(%(lotes)s)"
            params['lotes'] = lotes_list

    # --- Lógica para Saída para Montagem (Concluídos - 998) ---
    if fase == 998:
        if not all(table_exists(tbl) for tbl in ['toqmovi', 'reqordem']): return jsonify([])
        query = f"""
            WITH 
            ordens_produtos_relevantes AS (
                SELECT o.ordem, r.reqproduto
                FROM {fq('ordem')} o
                JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
                JOIN {fq('reqordem')} r ON r.reqord = o.ordem
                JOIN {fq('produto')} p ON p.produto = r.reqproduto
                WHERE r.rqoquanti > 0 AND r.reqfase = 17 {lote_filter_clause} AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
                  AND p.profantasm = 'N'
                  AND p.proorigem = 'F'
                  AND p.pronome NOT ILIKE '%%CANTONEIRA%%' 
                  AND p.pronome NOT ILIKE '%%(ALU)%%' 
                  AND p.pronome NOT ILIKE '%%(FERRO)%%'
                GROUP BY o.ordem, r.reqproduto
            ),
            movimentos_concluidos AS (
                SELECT m.priordem AS ordem, m.priproduto AS produto, SUM(m.priquanti) AS qtd_produzida, MAX(m.pridata) AS data_conclusao
                FROM {fq('toqmovi')} m
                JOIN ordens_produtos_relevantes opr ON TRIM(CAST(m.priordem AS TEXT)) = TRIM(CAST(opr.ordem AS TEXT)) AND TRIM(m.priproduto) = TRIM(opr.reqproduto)
                WHERE m.pritransac = '14' AND (m.priobserv IS NULL OR m.priobserv = '') AND m.pridata >= '2025-01-01'
                GROUP BY m.priordem, m.priproduto
            ),
            reserva_num AS (
                SELECT reqord, reqproduto, MAX(reqnumero) as reqnumero
                FROM {fq('reqordem')} r
                WHERE EXISTS (SELECT 1 FROM ordens_produtos_relevantes opr WHERE opr.ordem = r.reqord AND opr.reqproduto = r.reqproduto)
                GROUP BY reqord, reqproduto
            )
            SELECT mc.ordem, p.pronome as descricao, mc.qtd_produzida, o.ordquanti, l.lotdes as lote_descricao, mc.data_conclusao, res.reqnumero
            FROM movimentos_concluidos mc
            JOIN {fq('ordem')} o ON o.ordem = mc.ordem
            JOIN {fq('produto')} p ON p.produto = mc.produto
            JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
            LEFT JOIN reserva_num res ON res.reqord = mc.ordem AND res.reqproduto = mc.produto
            WHERE mc.qtd_produzida > 0 
            ORDER BY mc.data_conclusao DESC, mc.ordem
        """
        df, error = fetch_data_from_db(query, params=params)
        if error: return jsonify({"error": str(error)}), 500
        if not df.empty: df['data_conclusao'] = pd.to_datetime(df['data_conclusao'], errors='coerce').dt.strftime('%Y-%m-%d')
        return jsonify(df.fillna('').to_dict('records'))

    # --- Lógica para Saída para Pintura (Concluídos - 999) ---
    elif fase == 999:
        if not all(table_exists(tbl) for tbl in ['toqmovi', 'reqordem', 'processo']): return jsonify([])
        query = f"""
            WITH 
            ordens_produtos_relevantes AS (
                SELECT o.ordem, r.reqproduto
                FROM {fq('ordem')} o
                JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
                JOIN {fq('reqordem')} r ON r.reqord = o.ordem
                WHERE EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = r.reqproduto AND pr.fase IN (25, 30))
                AND r.rqoquanti > 0 AND r.reqproduto ILIKE 'OSS%%' {lote_filter_clause} AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
                GROUP BY o.ordem, r.reqproduto
            ),
            movimentos_concluidos AS (
                SELECT m.priordem AS ordem, m.priproduto AS produto, SUM(m.priquanti) AS qtd_produzida, MAX(m.pridata) AS data_conclusao
                FROM {fq('toqmovi')} m
                JOIN ordens_produtos_relevantes opr ON TRIM(CAST(m.priordem AS TEXT)) = TRIM(CAST(opr.ordem AS TEXT)) AND TRIM(m.priproduto) = TRIM(opr.reqproduto)
                WHERE m.pritransac = '14' AND (m.priobserv IS NULL OR m.priobserv = '') AND m.pridata >= '2025-01-01'
                GROUP BY m.priordem, m.priproduto
            ),
            reserva_num AS (
                SELECT reqord, reqproduto, MAX(reqnumero) as reqnumero
                FROM {fq('reqordem')} r
                WHERE EXISTS (SELECT 1 FROM ordens_produtos_relevantes opr WHERE opr.ordem = r.reqord AND opr.reqproduto = r.reqproduto)
                GROUP BY reqord, reqproduto
            )
            SELECT mc.ordem, p.pronome as descricao, mc.qtd_produzida, o.ordquanti, l.lotdes as lote_descricao, mc.data_conclusao, res.reqnumero
            FROM movimentos_concluidos mc
            JOIN {fq('ordem')} o ON o.ordem = mc.ordem
            JOIN {fq('produto')} p ON p.produto = mc.produto
            JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
            LEFT JOIN reserva_num res ON res.reqord = mc.ordem AND res.reqproduto = mc.produto
            WHERE mc.qtd_produzida > 0 ORDER BY mc.data_conclusao DESC, mc.ordem
        """
        df, error = fetch_data_from_db(query, params=params)
        if error: return jsonify({"error": str(error)}), 500
        if not df.empty: df['data_conclusao'] = pd.to_datetime(df['data_conclusao'], errors='coerce').dt.strftime('%Y-%m-%d')
        return jsonify(df.fillna('').to_dict('records'))

    # --- Lógica Padrão para os outros monitores (Concluídos) ---
    ord_col, qtd_col = _pasfase_columns()
    if fase == 35 and table_exists('planilha'):
        qtd_fase_source = f"""
            SELECT CAST(plaordem AS TEXT) AS ordem, SUM(COALESCE(CAST(plaquant AS NUMERIC), 0)) AS qtd_produzida
            FROM {fq('planilha')} WHERE plafase = %(fase)s GROUP BY plaordem
        """
    else:
        qtd_fase_source = f"""
            SELECT CAST({ord_col} AS TEXT) AS ordem, SUM(COALESCE({qtd_col}, 0)) AS qtd_produzida
            FROM {fq('pasfase')} WHERE fase = %(fase)s GROUP BY {ord_col}
        """

    query = f"""
        WITH qtd_fase_por_ordem AS ({qtd_fase_source}),
        ordens_filtradas AS (
            SELECT 
                o.ordem, o.ordproduto, o.ordquanti, o.orddtence,
                l.lotdes as lote_descricao
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            WHERE EXISTS (
                SELECT 1 FROM {fq('processo')} pr 
                WHERE pr.produto = o.ordproduto AND pr.fase = %(fase)s
            )
            {lote_filter_clause}
        )
        SELECT 
            of.ordem, p.pronome as descricao, 
            COALESCE(q.qtd_produzida, 0) AS qtd_produzida,
            of.ordquanti, of.lote_descricao,
            CASE 
                WHEN of.orddtence != DATE '0001-01-01' THEN of.orddtence 
                ELSE CURRENT_DATE 
            END AS data_conclusao
        FROM ordens_filtradas of
        JOIN {fq('produto')} p ON p.produto = of.ordproduto
        LEFT JOIN qtd_fase_por_ordem q ON CAST(of.ordem AS TEXT) = q.ordem
        WHERE COALESCE(q.qtd_produzida, 0) > 0 
        ORDER BY data_conclusao DESC, of.ordem
    """
    
    df, error = fetch_data_from_db(query, params=params)
    if error: 
        print(f"DB Error in /api/completed: {error}")
        return jsonify({"error": str(error)}), 500

    if not df.empty:
        df['data_conclusao'] = pd.to_datetime(df['data_conclusao'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    return jsonify(df.fillna('').to_dict('records'))

@app.route('/api/devolucoes', methods=['GET'])
def get_devolucoes_data():
    fase = request.args.get('fase', type=int)

    if fase not in [999, 25, 30]:
        return jsonify([])

    lot_table = 'loteprod' if table_exists('loteprod') else 'lotprod'
    required_tables = [lot_table, 'toqmovi', 'grmotper', 'produto', 'ordem', 'processo']
    if not all(table_exists(tbl) for tbl in required_tables):
        missing = [tbl for tbl in required_tables if not table_exists(tbl)]
        return jsonify({"error": f"Tabelas necessárias não encontradas: {', '.join(missing)}"}), 500

    op_filter = "(l.lotdes ILIKE '%%Petra%%' OR l.lotdes ILIKE '%%Solare%%' OR l.lotdes ILIKE '%%Garland%%')"

    phase_filter_clause = ""
    if fase == 25:
        phase_filter_clause = f"AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = s.priproduto AND pr.fase = 25)"
    elif fase == 30:
        phase_filter_clause = f"AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = s.priproduto AND pr.fase = 30)"


    query = f"""
        WITH
        movimentos AS (
            SELECT
                m.priordem,
                m.priproduto,
                m.priquanti,
                m.pritransac,
                m.pridata,
                CAST(SUBSTRING(m.priobserv FROM '\\*d:([0-9]+)') AS INTEGER) as motivo_codigo
            FROM {fq('toqmovi')} m
            WHERE m.priobserv ILIKE '%%*d:%%'
              AND m.pridata >= '2025-01-01'
              AND m.pritransac IN ('4', '14')
        ),
        saldos_por_motivo AS (
            SELECT
                priordem,
                priproduto,
                motivo_codigo,
                SUM(CASE WHEN pritransac = '4' THEN priquanti ELSE 0 END) as total_devolvido,
                SUM(CASE WHEN pritransac = '14' THEN priquanti ELSE 0 END) as total_debitado,
                MAX(CASE WHEN pritransac = '4' THEN pridata ELSE NULL END) as ultima_data_devolucao
            FROM movimentos
            GROUP BY priordem, priproduto, motivo_codigo
        )
        SELECT
            l.lotdes as lote_descricao,
            s.priordem as ordem,
            p.pronome as descricao,
            s.ultima_data_devolucao as data,
            GREATEST(s.total_devolvido - s.total_debitado, 0) as quantidade,
            gmp.gmpdescri as motivo
        FROM saldos_por_motivo s
        JOIN {fq('produto')} p ON TRIM(p.produto) = TRIM(s.priproduto)
        JOIN {fq('ordem')} o ON TRIM(CAST(o.ordem AS TEXT)) = TRIM(CAST(s.priordem AS TEXT))
        JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
        LEFT JOIN {fq('grmotper')} gmp ON gmp.gmpcodigo = s.motivo_codigo
        WHERE GREATEST(s.total_devolvido - s.total_debitado, 0) > 0 AND {op_filter}
        {phase_filter_clause}
        ORDER BY s.ultima_data_devolucao DESC;
    """

    df, error = fetch_data_from_db(query)
    if error:
        return jsonify({"error": str(error)}), 500

    if not df.empty:
        df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.strftime('%d/%m/%Y')
    
    return jsonify(df.fillna('').to_dict('records'))


@app.route('/api/export', methods=['GET'])
def export_data():
    fase = request.args.get('fase', default=5, type=int)
    status_param = request.args.get('status')
    if status_param not in ['delayed', 'ontime']: return "Status inválido", 400

    lot_table = 'loteprod' if table_exists('loteprod') else 'lotprod'
    if not table_exists(lot_table): return "Tabela de lote não encontrada", 500
    
    ord_col, qtd_col = _pasfase_columns()

    query = get_base_query(lot_table, ord_col, qtd_col, fase)
    df, error = fetch_data_from_db(query, params={'fase': fase})
    if error: return error, 500
    
    df_processed = process_data(df.copy(), fase)
    
    status_map = {'delayed': 'atrasado', 'ontime': 'em_dia'}
    target_status = status_map.get(status_param)
    
    #Para pintura, ontime inclui futuro
    if fase == 35 and status_param == 'ontime':
        final_df = df_processed[df_processed['status'].isin(['em_dia', 'futuro'])]
    else:
        final_df = df_processed[df_processed['status'] == target_status]


    export_columns = {
        'lote_descricao': 'Lote', 'ordem': 'Ordem', 'produto': 'Produto',
        'descricao': 'Descrição', 'saldo_pendente': 'Saldo', 
        'data_inicio_prevista': 'Início Previsto', 'data_fim_prevista': 'Fim Previsto'
    }
    cols_to_export = [col for col in export_columns.keys() if col in final_df.columns]
    final_df_export = final_df[cols_to_export].rename(columns=export_columns)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df_export.to_excel(writer, index=False, sheet_name='Relatorio')
    output.seek(0)
    
    phase_map = {
        5: 'Corte', 10: 'Prensa', 15: 'Usinagem', 25: 'Maciço', 30: 'Chapa', 35: 'Pintura', 136: 'Tapecaria',
        998: 'Saida_Montagem', 999: 'Saida_Pintura'
    }
    phase_name = phase_map.get(fase, 'Desconhecido')
    
    return send_file(output, as_attachment=True, download_name=f'relatorio_{phase_name.lower()}_{status_param}.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)

