from fastapi import APIRouter
from services.postgres import create_table_if_not_exists, insert_into_table

postgres_router = APIRouter(prefix='/postgres', tags = ['Everything related to Postgres'])

@postgres_router.get('/create-tables')
def create_tables():
    try: 
        table_names = create_table_if_not_exists()
        return {'status': f'All tables {table_names} are created successfully'}
    except Exception as e:
        print(e)
            
@postgres_router.get('/insert-into-table')
async def insert():
    try:
        # assuming that the table name is the same as file name
        
        # Menu table
        obj = {}
        tables = ['menu', 'members', 'sales']
        for i in tables:
            query = insert_into_table(i, f'{i}.csv')
            obj[i] = query
        
        return obj        
    except Exception as e:
        print(e)
        return e