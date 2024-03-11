from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()


class Logs(Base):
    __tablename__ = 'logs'

    id = Column(Integer(), primary_key=True)
    month = Column(String(200))
    day = Column(String(200))
    time = Column(String(200))
    user_name = Column(String(200))
    device = Column(String(200))
    process = Column(String(200))
    description = Column(String(700))


url = "postgresql+psycopg2://root:1111@localhost/db_logs2"


def createdb():
    if not database_exists(url):
        print("БД не существует")
        create_database(url)
        engine = create_engine(url)
        Base.metadata.create_all(engine)


def insertdata(month, day, time, user, device, process, description):
    engine = create_engine(url)
    log = Logs(
        month=month,
        day=day,
        time=time,
        user_name=user,
        device=device,
        process=process,
        description=description
    )

    session = sessionmaker(bind=engine)
    session = Session(bind=engine)
    session.add(log)
    session.commit()
    session.close()


def getdata():
    engine = create_engine(url)
    session = sessionmaker(bind=engine)
    session = Session(bind=engine)

    logs = session.query(Logs).all()
    for log in logs:
        print(log.id, log.month, log.day, log.time, log.user_name, log.device, log.description)
    session.close()



if __name__ == "__main__":
    createdb()
    getdata()

