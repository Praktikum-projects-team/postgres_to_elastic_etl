from dotenv import load_dotenv
from pydantic import (
    BaseSettings,
    Field,
)

load_dotenv()


class PostgresConfig(BaseSettings):
    host: str = Field(..., env='DB_HOST')
    user: str = Field(..., env='DB_USER')
    password: str = Field(..., env='DB_PASSWORD')
    database: str = Field(..., env='DB_NAME')
    port: int = Field(..., env='DB_PORT')




