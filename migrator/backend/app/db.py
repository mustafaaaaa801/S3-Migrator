from sqlmodel import SQLModel, create_engine, Session
from .config import settings

engine = create_engine(settings.DATABASE_URL, echo=False, connect_args={"check_same_thread": False} if 'sqlite' in settings.DATABASE_URL else {})

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    return Session(engine)