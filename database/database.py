import os

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import parameters as parameters

engine = create_engine(
    "sqlite:///"
    + os.path.dirname(os.path.abspath(__file__))
    + "/"
    + parameters.database
    + ".db"
)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
