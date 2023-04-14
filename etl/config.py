from dotenv import load_dotenv
from pydantic import (
    BaseSettings,
    Field,
    FilePath
)

load_dotenv()


class PostgresConfig(BaseSettings):
    host: str = Field(..., env='DB_HOST')
    user: str = Field(..., env='DB_USER')
    password: str = Field(..., env='DB_PASSWORD')
    database: str = Field(..., env='DB_NAME')
    port: int = Field(..., env='DB_PORT')


class MainConfig(BaseSettings):
    es_host: str = Field(..., env='ES_HOST')
    batch_size: int = Field(..., env='BATCH_SIZE')
    state_file: str = Field(..., env='STATE_FILE')
    etl_run_interval: int = Field(..., env='ETL_RUN_INTERVAL')  # seconds

