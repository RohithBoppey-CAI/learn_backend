# the idea is to just run this files and the tables need to be created - or the API endpoint triggering needs to be create the databases

from ..schema import Base
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from settings import settings

SQLALCHEMY_DATABASE_URL = (
    "postgresql://"
    + settings.POSTGRES_USER
    + ":"
    + settings.POSTGRES_PASSWORD
    + "@"
    + settings.POSTGRES_SERVICE
    + ":"
    + settings.POSTGRES_PORT
    + "/"
    + settings.POSTGRES_DB
)


# Create an engine for the database
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Create the Session class to interact with the database
Session = sessionmaker(bind=engine)
session = Session()


def create_table_if_not_exists():
    # Use SQLAlchemy's Inspector to check if the table exists
    inspector = inspect(engine)
    
    Base.metadata.create_all(engine)
    return inspector.get_table_names()
    
    # Check if the table exists in the database
    # if "menu" not in inspector.get_table_names():
    #     print("Table 'menu' does not exist. Creating it now...")
    #     # Create the table
    #     Base.metadata.create_all(engine)
    #     return inspector.get_table_names()
    # else:
    #     print("Table 'menu' already exists.")

if __name__ == "__main__":
    create_table_if_not_exists()


