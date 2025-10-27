from config import fq, table_exists, _pasfase_columns
from data_processing import process_data_generic

def get_query(fq, lot_table, ord_col, qtd_col):
    """
    Gera a query SQL para o monitor Garland, que consolida OPs de Garland,
    além de itens específicos de Solare e Petra com prioridade de embarque.
    """
    if not all(table_exists(tbl) for tbl in ['ordem', lot_table, 'produto', 'toqmovi']):
        return "SELECT 1 WHERE 1=0"

    return f"""
        WITH OPs_Prioritarias AS (
            -- Seleciona todas as ordens de Garland com prioridade '1'
            SELECT o.ordem
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN {fq('produto')} p ON o.ordproduto = p.produto
            WHERE l.lotdes ILIKE '%Garland%' AND p.prodpriem = '1'

            UNION

            -- Seleciona ordens de Solare para produtos 'LERIADO' com prioridade '1'
            SELECT o.ordem
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN {fq('produto')} p ON o.ordproduto = p.produto
            WHERE l.lotdes ILIKE '%Solare%'
              AND p.pronome ILIKE '%LERIADO%'
              AND p.prodpriem = '1'

            UNION

            -- Seleciona ordens de Petra para produtos que contêm na descrição os códigos específicos com prioridade '1'
            SELECT o.ordem
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN {fq('produto')} p ON o.ordproduto = p.produto
            WHERE l.lotdes ILIKE '%Petra%'
              AND (p.pronome ILIKE '%PT105%' OR p.pronome ILIKE '%PT102%' OR p.pronome ILIKE '%PT107%'
                   OR p.pronome ILIKE '%PF107%' OR p.pronome ILIKE '%PT100%')
              AND p.prodpriem = '1'
        ),
        Qtd_Produzida AS (
            -- Calcula o débito da produção (transação 3 na toqmovi)
            SELECT
                TRIM(CAST(m.priordem AS TEXT)) as ordem,
                SUM(COALESCE(m.priquanti, 0)) as qtd_produzida
            FROM {fq('toqmovi')} m
            WHERE m.pritransac = '3' -- Baixa de produção
            GROUP BY m.priordem
        ),
        total_historico_por_lote AS (
            -- Agrupa a quantidade total planejada por lote para cálculo de percentual
            SELECT 
                l.lotdes, 
                SUM(o.ordquanti) as total_qty_lote
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON l.lotcod = o.lotcod
            WHERE EXISTS (SELECT 1 FROM OPs_Prioritarias op WHERE op.ordem = o.ordem)
            GROUP BY l.lotdes
        )
        -- Query final que junta as informações e calcula o saldo
        SELECT
            o.ordem,
            o.ordproduto AS produto,
            p.pronome AS descricao,
            GREATEST(o.ordquanti - COALESCE(qp.qtd_produzida, 0), 0) AS saldo_pendente,
            o.ordquanti,
            o.orddtence,
            l.lotdes AS lote_descricao,
            l.lottrans AS lote_trans,
            l.lotdtini,
            l.lotdtpre,
            th.total_qty_lote as total_historico_lote,
            0 as devolucao_saldo -- Coluna padrão, não usada aqui
        FROM {fq('ordem')} o
        JOIN OPs_Prioritarias op ON o.ordem = op.ordem
        JOIN {fq('produto')} p ON o.ordproduto = p.produto
        JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
        LEFT JOIN Qtd_Produzida qp ON TRIM(CAST(o.ordem AS TEXT)) = qp.ordem
        LEFT JOIN total_historico_por_lote th ON th.lotdes = l.lotdes
        WHERE GREATEST(o.ordquanti - COALESCE(qp.qtd_produzida, 0), 0) > 0
          AND o.orddtence = DATE '0001-01-01' -- Considera apenas ordens em aberto
    """

def get_completed_query(fq, lot_table, ord_col, qtd_col, lote_filter_clause=""):
    """Query para dados concluídos do monitor de Garland."""
    
    if not all(table_exists(tbl) for tbl in ['ordem', lot_table, 'produto', 'toqmovi']):
        return "SELECT 1 WHERE 1=0"

    return f"""
        WITH OPs_Prioritarias AS (
            -- Seleciona todas as ordens de Garland com prioridade '1'
            SELECT o.ordem
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN {fq('produto')} p ON o.ordproduto = p.produto
            WHERE l.lotdes ILIKE '%Garland%' AND p.prodpriem = '1'

            UNION

            -- Seleciona ordens de Solare para produtos 'LERIADO' com prioridade '1'
            SELECT o.ordem
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN {fq('produto')} p ON o.ordproduto = p.produto
            WHERE l.lotdes ILIKE '%Solare%'
              AND p.pronome ILIKE '%LERIADO%'
              AND p.prodpriem = '1'

            UNION

            -- Seleciona ordens de Petra para produtos que contêm na descrição os códigos específicos com prioridade '1'
            SELECT o.ordem
            FROM {fq('ordem')} o
            JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
            JOIN {fq('produto')} p ON o.ordproduto = p.produto
            WHERE l.lotdes ILIKE '%Petra%'
              AND (p.pronome ILIKE '%PT105%' OR p.pronome ILIKE '%PT102%' OR p.pronome ILIKE '%PT107%'
                   OR p.pronome ILIKE '%PF107%' OR p.pronome ILIKE '%PT100%')
              AND p.prodpriem = '1'
        ),
        Qtd_Produzida AS (
            -- Calcula o débito da produção (transação 3 na toqmovi)
            SELECT
                TRIM(CAST(m.priordem AS TEXT)) as ordem,
                SUM(COALESCE(m.priquanti, 0)) as qtd_produzida
            FROM {fq('toqmovi')} m
            WHERE m.pritransac = '3' -- Baixa de produção
              AND EXISTS (SELECT 1 FROM OPs_Prioritarias op WHERE op.ordem = m.priordem)
            GROUP BY m.priordem
        )
        SELECT 
            o.ordem, 
            p.pronome as descricao, 
            COALESCE(qp.qtd_produzida, 0) AS qtd_produzida,
            o.ordquanti, 
            l.lotdes as lote_descricao,
            CASE 
                WHEN o.orddtence != DATE '0001-01-01' THEN o.orddtence 
                ELSE CURRENT_DATE 
            END AS data_conclusao
        FROM {fq('ordem')} o
        JOIN OPs_Prioritarias op ON o.ordem = op.ordem
        JOIN {fq('produto')} p ON o.ordproduto = p.produto
        JOIN {fq(lot_table)} l ON o.lotcod = l.lotcod
        LEFT JOIN Qtd_Produzida qp ON TRIM(CAST(o.ordem AS TEXT)) = qp.ordem
        WHERE COALESCE(qp.qtd_produzida, 0) > 0
        {lote_filter_clause}
        ORDER BY data_conclusao DESC, o.ordem
    """

def process_data(df, fase):
    """Processa dados específicos do monitor de Garland."""
    return process_data_generic(df, fase)
