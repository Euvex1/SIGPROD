import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Load environment variables from .env at project root
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

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
    """Retorna o nome da tabela com schema qualificado."""
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

def get_lot_table():
    """Retorna o nome da tabela de lote apropriada."""
    return 'loteprod' if table_exists('loteprod') else 'lotprod'
