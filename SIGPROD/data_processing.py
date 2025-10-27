import pandas as pd
import re

def _parse_phase_dates(text: str, phase_name: str):
    """Parse de datas para fases específicas."""
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

def process_data_generic(df: pd.DataFrame, fase: int) -> pd.DataFrame:
    """Processamento genérico de dados para a maioria dos monitores."""
    if df.empty: return df

    # O nome da fase para o parsing das datas pode ser diferente do nome do monitor.
    phase_parse_key_map = {
        5: 'CORTE',
        10: 'PRENSA',
        15: 'USINAGEM',
        25: 'MONTAGEM',
        30: 'MONTAGEM',
        35: 'ACABAMENTO',
        40: 'GARLANDACABAMENTO',  # Template para o monitor Garland
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

def format_dataframe_for_json(df: pd.DataFrame, is_grouped: bool = False):
    """Formata DataFrame para retorno JSON."""
    if df.empty:
        return {"is_grouped": is_grouped, "data": []} if not is_grouped else {"is_grouped": True, "summary": [], "details": []}
    
    # Ordenação
    sort_cols = ['corte_dtini']
    if 'ordem' in df.columns: 
        sort_cols.append('ordem')
    else: 
        sort_cols.append('lote_descricao')
    
    df.sort_values(by=sort_cols, na_position='last', inplace=True)
    
    # Formatação de datas
    date_cols = [col for col in ['orddtprev', 'orddtence', 'lotdtini', 'lotdtpre', 'corte_dtini', 'data_inicio_prevista', 'data_fim_prevista'] if col in df.columns]
    for col in date_cols:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    
    return df.fillna('').to_dict('records')
