import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session as _Session

_logger = logging.getLogger(__name__)

engine = create_engine(os.getenv("SQL_URI"), echo=False, pool_size=60, max_overflow=120, pool_pre_ping=True)
Base = declarative_base()


class SafeSession(_Session):
    """
    Session subclass that ensures a rollback is attempted on exception
    and always closes the connection when exiting a context. This prevents
    returning connections in an aborted transaction state to the pool.
    """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If an exception occurred, try to rollback to clear the transaction
        if exc_type is not None:
            try:
                self.rollback()
            except Exception:
                pass
        # Always close the session to return connection to the pool
        try:
            self.close()
        except Exception:
            pass
        # Do not suppress exceptions
        return False

# Use SafeSession as the session class for sessionmaker
session_maker = sessionmaker(bind=engine, class_=SafeSession, expire_on_commit=False)


def create_all(drop=False):
    if drop:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
