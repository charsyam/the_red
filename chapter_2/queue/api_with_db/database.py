from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()
Session = sessionmaker()


def init_database(url: str):
    engine = create_engine(url)
    Base.metadata.bind = engine
    Session.configure(bind=engine)
