from sqlalchemy import create_engine
from sqlalchemy_utils import drop_database

url = "postgresql+psycopg2://root:1111@localhost/db_logs2"

engine = create_engine(url)
drop_database(engine.url)
