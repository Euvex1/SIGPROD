from config import fq, table_exists, _pasfase_columns
from data_processing import process_data_generic

def get_query(fq, lot_table, ord_col, qtd_col):
    """Gera a query SQL para o monitor de usinagem."""
    op_filter = "l.lotdes ILIKE '%%OSSO%%' AND l.lotdes NOT ILIKE '%%AVULSO%%'"
    
    # Para usinagem, inclui perdas
    perdas_cte = f""",
        qtd_perdida AS (
            SELECT CAST(perofscod AS TEXT) as ordem, SUM(COALESCE(perqtdper, 0)) as qtd_perdida
            FROM {fq('perdas')}
            GROUP BY perofscod
        )
        """
    
    qtd_fase_source = f"""
        SELECT CAST({ord_col} AS TEXT) AS ordem, SUM(COALESCE({qtd_col}, 0)) AS qtd
        FROM {fq('pasfase')} WHERE fase = %(fase)s GROUP BY {ord_col}
    """
    
    return f"""
        WITH qtd_fase AS ({qtd_fase_source})
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
                GREATEST(o.ordquanti - (COALESCE(q.qtd, 0) + COALESCE(pds.qtd_perdida, 0)), 0) AS saldo_pendente,
                0 as devolucao_saldo,
                o.ordquanti, o.orddtence, l.lotdes AS lote_descricao,
                l.lottrans AS lote_trans, l.lotdtini, l.lotdtpre
            FROM {fq('ordem')} o
            JOIN {fq('produto')} p ON p.produto = o.ordproduto
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            LEFT JOIN qtd_fase q ON CAST(o.ordem AS TEXT) = q.ordem
            LEFT JOIN qtd_perdida pds ON CAST(o.ordem AS TEXT) = pds.ordem
            WHERE o.orddtence = DATE '0001-01-01'
              AND {op_filter}
              AND EXTRACT(YEAR FROM l.lotdtini) >= 2025
              AND GREATEST(o.ordquanti - (COALESCE(q.qtd, 0) + COALESCE(pds.qtd_perdida, 0)), 0) > 0
              AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = o.ordproduto AND pr.fase = %(fase)s)
        )
        SELECT df.*, th.total_qty_lote as total_historico_lote
        FROM dados_filtrados df
        LEFT JOIN total_historico_por_lote th ON th.lotdes = df.lote_descricao
    """

def get_completed_query(fq, lot_table, ord_col, qtd_col, lote_filter_clause=""):
    """Query para dados concluídos do monitor de usinagem."""
    qtd_fase_source = f"""
        SELECT CAST({ord_col} AS TEXT) AS ordem, SUM(COALESCE({qtd_col}, 0)) AS qtd_produzida
        FROM {fq('pasfase')} WHERE fase = %(fase)s GROUP BY {ord_col}
    """

    return f"""
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

def process_data(df, fase):
    """Processa dados específicos do monitor de usinagem."""
    return process_data_generic(df, fase)
