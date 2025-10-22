def get_query(fq, lot_table, table_exists, ord_col, qtd_col):
    """Gera a query SQL para o monitor de pintura."""

    op_filter = "(l.lotdes ILIKE '%%Petra%%' OR l.lotdes ILIKE '%%Solare%%' OR l.lotdes ILIKE '%%Garland%%')"

    # Lógica Padrão para os outros monitores
    devolucoes_cte = ""
    perdas_cte = ""
    saldo_pendente_calc = "GREATEST(o.ordquanti - COALESCE(q.qtd, 0), 0)"
    devolucao_saldo_select = "0 as devolucao_saldo,"
    devolucao_join = ""
    perdas_join = ""
    ordem_status_filter = "o.orddtence = DATE '0001-01-01'"

    # Pintura (fase 35) usa a tabela 'planilha' se existir
    if table_exists('planilha'):
        qtd_fase_source = f"""
            SELECT CAST(plaordem AS TEXT) AS ordem, SUM(COALESCE(CAST(plaquant AS NUMERIC), 0)) AS qtd
            FROM {fq('planilha')} WHERE plafase = %(fase)s GROUP BY plaordem
        """
    else: # Fallback para pasfase se planilha não existir
        qtd_fase_source = f"""
            SELECT CAST({ord_col} AS TEXT) AS ordem, SUM(COALESCE({qtd_col}, 0)) AS qtd
            FROM {fq('pasfase')} WHERE fase = %(fase)s GROUP BY {ord_col}
        """

    base_join = "LEFT JOIN qtd_fase q ON CAST(o.ordem AS TEXT) = q.ordem"

    return f"""
        WITH qtd_fase AS ({qtd_fase_source})
        {devolucoes_cte}
        {perdas_cte},
        total_historico_por_lote AS (
            SELECT l.lotdes, SUM(o.ordquanti) as total_qty_lote
            FROM {fq('ordem')} o JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
            WHERE {op_filter}
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
              AND ({saldo_pendente_calc}) > 0
              AND EXISTS (SELECT 1 FROM {fq('processo')} pr WHERE pr.produto = o.ordproduto AND pr.fase = %(fase)s)
        )
        SELECT df.*, th.total_qty_lote as total_historico_lote
        FROM dados_filtrados df
        LEFT JOIN total_historico_por_lote th ON th.lotdes = df.lote_descricao
    """
