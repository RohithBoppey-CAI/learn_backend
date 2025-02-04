from fastapi import APIRouter
from services.postgres import create_table_if_not_exists, insert_into_table, caller
from services.pydantic import DannyDinerRequest

postgres_router = APIRouter(prefix='/postgres', tags = ['Everything related to Postgres'])

@postgres_router.get('/create-tables')
def create_tables():
    try: 
        table_names = create_table_if_not_exists()
        return {'status': f'All tables {table_names} are created successfully'}
    except Exception as e:
        print(e)
            
@postgres_router.get('/insert-into-table')
def insert():
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
    
    
@postgres_router.post('/find-answer')
async def give_answer(req: DannyDinerRequest):
    try:
        print(f'You are requesting for {req.question_number}')
        fn = caller(req.question_number)
        
        # take in the parameter and then use the appropriate function name
        res = await fn()
        return res
        
    except Exception as e:
        print(e)