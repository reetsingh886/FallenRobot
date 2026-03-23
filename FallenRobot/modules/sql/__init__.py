from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from FallenRobot import DB_URI
from FallenRobot import LOGGER as log

BASE = declarative_base()
SESSION = None  # default safe


def start():
    global SESSION

    if not DB_URI:
        log.warning("[PostgreSQL] DATABASE_URL not found, skipping DB...")
        return None

    try:
        if DB_URI.startswith("postgres://"):
            db_uri = DB_URI.replace("postgres://", "postgresql://", 1)
        else:
            db_uri = DB_URI

        engine = create_engine(db_uri, client_encoding="utf8")
        log.info("[PostgreSQL] Connecting to database...")

        BASE.metadata.bind = engine
        BASE.metadata.create_all(engine)

        SESSION = scoped_session(sessionmaker(bind=engine, autoflush=False))
        log.info("[PostgreSQL] Connection successful")
        return SESSION

    except Exception as e:
        log.error(f"[PostgreSQL] Failed: {e}")
        return None


# start safely
start()
