from config import fq, table_exists, _pasfase_columns
from data_processing import process_data_generic

def get_query(fq, lot_table, ord_col, qtd_col):
    """Gera a query SQL para o monitor de corte com lógica específica para produtos com fases 5 e 13."""
    op_filter = "l.lotdes ILIKE '%%OSSO%%' AND l.lotdes NOT ILIKE '%%AVULSO%%'"
    
    return f"""
        WITH produtos_fases AS (
            SELECT DISTINCT
                pr.produto,
                CASE 
                    WHEN EXISTS (SELECT 1 FROM {fq('processo')} pr13 WHERE pr13.produto = pr.produto AND pr13.fase = 13)
                         AND EXISTS (SELECT 1 FROM {fq('processo')} pr5 WHERE pr5.produto = pr.produto AND pr5.fase = 5)
                    THEN 13  -- Produto tem ambas as fases, usa fase 13 para baixa
                    ELSE 5   -- Produto tem apenas fase 5, usa fase 5 para baixa
                END as fase_baixa
            FROM {fq('processo')} pr
            WHERE pr.fase = 5
        ),
        qtd_fase AS (
            SELECT 
                CAST(pf.{ord_col} AS TEXT) AS ordem, 
                SUM(COALESCE(pf.{qtd_col}, 0)) AS qtd
            FROM {fq('pasfase')} pf
            JOIN produtos_fases prf ON prf.fase_baixa = pf.fase
            JOIN {fq('ordem')} o ON o.ordem = pf.{ord_col} AND o.ordproduto = prf.produto
            GROUP BY pf.{ord_col}
        ),
        total_historico_por_lote AS (
            SELECT l.lotdes, SUM(o.ordquanti) as total_qty_lote
            FROM {fq('ordem')} o JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
            WHERE {op_filter}
            AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
            AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = o.ordproduto AND pr.fase = 5)
            GROUP BY l.lotdes
        )
        SELECT DISTINCT
            o.ordem, o.ordproduto AS produto, p.pronome AS descricao,
            GREATEST(o.ordquanti - COALESCE(q.qtd, 0), 0) AS saldo_pendente,
            0 as devolucao_saldo,
            o.ordquanti, o.orddtence, l.lotdes AS lote_descricao,
            l.lottrans AS lote_trans, l.lotdtini, l.lotdtpre,
            prf.fase_baixa,
            th.total_qty_lote as total_historico_lote
        FROM {fq('ordem')} o
        JOIN {fq('produto')} p ON p.produto = o.ordproduto
        JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
        JOIN produtos_fases prf ON prf.produto = o.ordproduto
        LEFT JOIN qtd_fase q ON CAST(o.ordem AS TEXT) = q.ordem
        LEFT JOIN total_historico_por_lote th ON th.lotdes = l.lotdes
        WHERE o.orddtence = DATE '0001-01-01'
          AND {op_filter}
          AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
          AND GREATEST(o.ordquanti - COALESCE(q.qtd, 0), 0) > 0
          AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = o.ordproduto AND pr.fase = 5)
        ORDER BY o.ordem, o.ordproduto
    """

def get_completed_query(fq, lot_table, ord_col, qtd_col, lote_filter_clause=""):
    """Query para dados concluídos do monitor de corte com lógica específica para produtos com fases 5 e 13."""
    return f"""
        WITH produtos_fases AS (
            SELECT DISTINCT
                pr.produto,
                CASE 
                    WHEN EXISTS (SELECT 1 FROM {fq('processo')} pr13 WHERE pr13.produto = pr.produto AND pr13.fase = 13)
                         AND EXISTS (SELECT 1 FROM {fq('processo')} pr5 WHERE pr5.produto = pr.produto AND pr5.fase = 5)
                    THEN 13  -- Produto tem ambas as fases, usa fase 13 para baixa
                    ELSE 5   -- Produto tem apenas fase 5, usa fase 5 para baixa
                END as fase_baixa
            FROM {fq('processo')} pr
            WHERE pr.fase = 5
        ),
        qtd_fase_por_ordem AS (
            SELECT 
                CAST(pf.{ord_col} AS TEXT) AS ordem, 
                SUM(COALESCE(pf.{qtd_col}, 0)) AS qtd_produzida
            FROM {fq('pasfase')} pf
            JOIN produtos_fases prf ON prf.fase_baixa = pf.fase
            JOIN {fq('ordem')} o ON o.ordem = pf.{ord_col} AND o.ordproduto = prf.produto
            GROUP BY pf.{ord_col}
        )
        SELECT DISTINCT
            of.ordem, p.pronome as descricao, 
            COALESCE(q.qtd_produzida, 0) AS qtd_produzida,
            of.ordquanti, of.lote_descricao,
            CASE 
                WHEN of.orddtence != DATE '0001-01-01' THEN of.orddtence 
                ELSE CURRENT_DATE 
            END AS data_conclusao
        FROM (
            SELECT 
                o.ordem, o.ordproduto, o.ordquanti, o.orddtence,
                l.lotdes as lote_descricao
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN produtos_fases prf ON prf.produto = o.ordproduto
            WHERE EXISTS (
                SELECT 1 FROM {fq('processo')} pr 
                WHERE pr.produto = o.ordproduto AND pr.fase = 5
            )
            {lote_filter_clause}
        ) of
        JOIN {fq('produto')} p ON p.produto = of.ordproduto
        LEFT JOIN qtd_fase_por_ordem q ON CAST(of.ordem AS TEXT) = q.ordem
        WHERE COALESCE(q.qtd_produzida, 0) > 0 
        ORDER BY data_conclusao DESC, of.ordem
    """

def process_data(df, fase):
    """Processa dados específicos do monitor de corte."""
    return process_data_generic(df, fase)
