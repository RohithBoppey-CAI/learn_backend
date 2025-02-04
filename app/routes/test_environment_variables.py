from fastapi import APIRouter
import os


env_router = APIRouter(prefix='/env', tags=['Test All the environment'])

@env_router.get("/test_env")
def get_env():
    # OS.get_env is the variable if found from the .env file
    # if the variable is not found, we can use this
    return {"existing_variable": os.getenv("FIRST_ENV"), 
            "non_existing_variable": os.getenv("SECOND_ENV", "NO VARIABLE FOUND")}    
    


@env_router.get("/give_all_envs")
def get_all_env():
    # once the env is loaded inside the application using dotenv module, you can find the env inside this
    return {'environ_object': os.environ}  
