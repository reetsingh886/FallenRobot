from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from FallenRobot import DB_URI
from FallenRobot import LOGGER as log

BASE = declarative_base()

def start():
    if not DB_URI:
        log.warning("[PostgreSQL] No DATABASE_URL found, skipping DB...")
        return None

    if DB_URI.startswith("postgres://"):
        db_uri = DB_URI.replace("postgres://", "postgresql://", 1)
    else:
        db_uri = DB_URI

    engine = create_engine(db_uri, client_encoding="utf8")
    log.info("[PostgreSQL] Connecting to database......")
    BASE.metadata.bind = engine
    BASE.metadata.create_all(engine)
    return scoped_session(sessionmaker(bind=engine, autoflush=False))


try:
    SESSION = start()
except Exception as e:
    log.exception(f"[PostgreSQL] Failed to connect due to {e}")
    SESSION = None

if SESSION:
    log.info("[PostgreSQL] Connection successful")
else:
    log.warning("[PostgreSQL] Running without database")
