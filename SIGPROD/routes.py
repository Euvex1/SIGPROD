from flask import jsonify, request, send_file, render_template, send_from_directory
import pandas as pd
import io
from config import fq, table_exists, _pasfase_columns, fetch_data_from_db, get_lot_table
from data_processing import format_dataframe_for_json

# Importar todos os módulos de monitor
from monitors import corte, prensa, usinagem, macico, chapa, saida_montagem, saida_pintura, pintura, tapecaria, garland

def register_routes(app):
    """Registra todas as rotas da aplicação."""
    
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

    @app.route('/monitor_garland')
    def monitor_garland_page():
        return render_template('garland.html')
    
    @app.route('/api/garland_data', methods=['GET'])
    def get_garland_data():
        """API específica para o monitor Garland usando fase 35"""
        lot_table = get_lot_table()
        if not table_exists(lot_table): 
            return jsonify({"error": f"Tabela de lote '{lot_table}' não encontrada"}), 500
        
        ord_col, qtd_col = _pasfase_columns()
        query = garland.get_query(fq, lot_table, ord_col, qtd_col)
        df, error = fetch_data_from_db(query, params={'fase': 35})
        if error: 
            return jsonify({"error": error}), 500
        
        if df is None or df.empty:
            return jsonify({"is_grouped": False, "data": []})

        df_processed = garland.process_data(df.copy(), 35)

        # Para Garland, aplicar agrupamento como outros monitores
        if not df_processed.empty:
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
                    if 'ordem' in df_to_format.columns: 
                        sort_cols.append('ordem')
                    else: 
                        sort_cols.append('lote_descricao')
                    
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

        return jsonify({
            "is_grouped": False,
            "data": df_processed.fillna('').to_dict('records')
        })

    @app.route('/static/<path:filename>')
    def static_files(filename):
        return send_from_directory('static', filename)

    # --- Rotas da API ---
    @app.route('/api/data', methods=['GET'])
    def get_production_data():
        fase = request.args.get('fase', default=5, type=int)
        lot_table = get_lot_table()
        if not table_exists(lot_table): 
            return jsonify({"error": f"Tabela de lote '{lot_table}' não encontrada"}), 500
        
        ord_col, qtd_col = _pasfase_columns()

        # Selecionar o módulo correto baseado na fase
        monitor_modules = {
            5: corte,
            10: prensa,
            15: usinagem,
            25: macico,
            30: chapa,
            35: pintura,
            40: garland,
            136: tapecaria,
            998: saida_montagem,
            999: saida_pintura
        }
        
        monitor_module = monitor_modules.get(fase)
        if not monitor_module:
            return jsonify({"error": f"Monitor não encontrado para fase {fase}"}), 400

        query = monitor_module.get_query(fq, lot_table, ord_col, qtd_col)
        df, error = fetch_data_from_db(query, params={'fase': fase})
        if error: 
            return jsonify({"error": error}), 500
        
        if df is None or df.empty:
            return jsonify({"is_grouped": False, "data": []})

        df_processed = monitor_module.process_data(df.copy(), fase)

        # Para os monitores que agrupam OPs (Maciço, Chapa, Pintura, Saída Pintura, Saída Montagem, Garland)
        if fase in [25, 30, 35, 40, 998, 999, 136] and not df_processed.empty:
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
                    if 'ordem' in df_to_format.columns: 
                        sort_cols.append('ordem')
                    else: 
                        sort_cols.append('lote_descricao')
                    
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
        lot_table = get_lot_table()
        if not table_exists(lot_table): 
            return jsonify({"error": f"Tabela de lote '{lot_table}' não encontrada"}), 500
        
        params = {'fase': fase}
        lote_filter_clause = ""
        if lotes_param:
            lotes_list = [lote.strip() for lote in lotes_param.split(',') if lote.strip()]
            if lotes_list:
                lote_filter_clause = "AND l.lotdes = ANY(%(lotes)s)"
                params['lotes'] = lotes_list

        # Selecionar o módulo correto baseado na fase
        monitor_modules = {
            5: corte,
            10: prensa,
            15: usinagem,
            25: macico,
            30: chapa,
            35: pintura,
            40: garland,
            136: tapecaria,
            998: saida_montagem,
            999: saida_pintura
        }
        
        monitor_module = monitor_modules.get(fase)
        if not monitor_module:
            return jsonify({"error": f"Monitor não encontrado para fase {fase}"}), 400

        ord_col, qtd_col = _pasfase_columns()
        query = monitor_module.get_completed_query(fq, lot_table, ord_col, qtd_col, lote_filter_clause)
        
        df, error = fetch_data_from_db(query, params=params)
        if error: 
            return jsonify({"error": str(error)}), 500
        
        if not df.empty: 
            df['data_conclusao'] = pd.to_datetime(df['data_conclusao'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        return jsonify(df.fillna('').to_dict('records'))

    @app.route('/api/devolucoes', methods=['GET'])
    def get_devolucoes_data():
        fase = request.args.get('fase', type=int)

        if fase not in [999, 25, 30]:
            return jsonify([])

        lot_table = get_lot_table()
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
        if status_param not in ['delayed', 'ontime']: 
            return "Status inválido", 400

        lot_table = get_lot_table()
        if not table_exists(lot_table): 
            return "Tabela de lote não encontrada", 500
        
        ord_col, qtd_col = _pasfase_columns()

        # Selecionar o módulo correto baseado na fase
        monitor_modules = {
            5: corte,
            10: prensa,
            15: usinagem,
            25: macico,
            30: chapa,
            35: pintura,
            40: garland,
            136: tapecaria,
            998: saida_montagem,
            999: saida_pintura
        }
        
        monitor_module = monitor_modules.get(fase)
        if not monitor_module:
            return f"Monitor não encontrado para fase {fase}", 400

        query = monitor_module.get_query(fq, lot_table, ord_col, qtd_col)
        df, error = fetch_data_from_db(query, params={'fase': fase})
        if error: 
            return error, 500
        
        df_processed = monitor_module.process_data(df.copy(), fase)
        
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
            5: 'Corte', 10: 'Prensa', 15: 'Usinagem', 25: 'Maciço', 30: 'Chapa', 35: 'Pintura', 40: 'Garland', 136: 'Tapecaria',
            998: 'Saida_Montagem', 999: 'Saida_Pintura'
        }
        phase_name = phase_map.get(fase, 'Desconhecido')
        
        return send_file(output, as_attachment=True, download_name=f'relatorio_{phase_name.lower()}_{status_param}.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

