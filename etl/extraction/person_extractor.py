import datetime
import logging
from typing import Generator

import backoff
import psycopg2
from psycopg2.extras import RealDictCursor

from config import PostgresConfig
from extraction.sql_statements import fw_statement, person_fw_statement, genre_fw_statement, person_statement
from extraction.state import State

