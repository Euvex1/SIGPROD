from config import fq, table_exists, _pasfase_columns
from data_processing import process_data_generic

def get_query(fq, lot_table, ord_col, qtd_col):
    """Gera a query SQL para o monitor de saída para montagem."""
    if not all(table_exists(tbl) for tbl in ['reqordem', 'toqmovi']): 
        return "SELECT 1 WHERE 1=0"
    
    op_filter = "(l.lotdes ILIKE '%%Petra%%' OR l.lotdes ILIKE '%%Solare%%' OR l.lotdes ILIKE '%%Garland%%')"
    
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

def get_completed_query(fq, lot_table, ord_col, qtd_col, lote_filter_clause=""):
    """Query para dados concluídos do monitor de saída para montagem."""
    if not all(table_exists(tbl) for tbl in ['toqmovi', 'reqordem']): 
        return "SELECT 1 WHERE 1=0"
    
    return f"""
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

def process_data(df, fase):
    """Processa dados específicos do monitor de saída para montagem."""
    return process_data_generic(df, fase)
