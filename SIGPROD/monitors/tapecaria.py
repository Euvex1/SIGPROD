import pandas as pd
import re

def get_query(fq, lot_table, table_exists):
    """Gera a query SQL para o monitor de tapeçaria."""
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

def process_data(df: pd.DataFrame):
    """Processa os dados especificamente para o monitor de tapeçaria."""
    if df.empty: return df

    # 1. Parse de Datas
    if 'lote_trans' in df.columns:
        dates = df['lote_trans'].fillna('').apply(_parse_tapecaria_dates)
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
    
    # 3. Lógica de filtragem específica para tapeçaria
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
