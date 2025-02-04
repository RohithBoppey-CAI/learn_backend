from fastapi import APIRouter
import os
from services import psql_execute_single, exec_one

from sqlalchemy import text

testing_router = APIRouter(prefix='/env', tags=['Test All the environments'])

@testing_router.get("/test_env")
def get_env():
    # OS.get_env is the variable if found from the .env file
    # if the variable is not found, we can use this
    return {"existing_variable": os.getenv("FIRST_ENV"), 
            "non_existing_variable": os.getenv("SECOND_ENV", "NO VARIABLE FOUND")}    
    


@testing_router.get("/give_all_envs")
def get_all_env():
    # once the env is loaded inside the application using dotenv module, you can find the env inside this
    return {'environ_object': os.environ}  


@testing_router.get("/test_postgres_connection")
async def test_postgres_connection():
    query = text("select * from sample_table")
    res = await psql_execute_single(query)
    res = exec_one(res)
    return res