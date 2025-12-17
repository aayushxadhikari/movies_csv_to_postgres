from sqlalchemy import create_engine
from etl.config import PG_CONFIG

DATABASE_URL = (
    f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
    f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['db']}"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
