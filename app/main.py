from fastapi import FastAPI
from routes import homepage_router, testing_router, postgres_router
from dotenv import load_dotenv

from settings import settings

# initialising the app in here
# note! This variable should always be "app" only, but not any other thing 
# if still you're changing the variable -> make sure you change in the uvicorn app as well:  uvicorn --reload main:main_app --log-level debug

# One way to use ENV variables: 
# by enabling this, we are storing the env variables inside the application state
load_dotenv()

# We can also use Settings in pydantic to see all the required env variables are present or not

main_app = FastAPI()

@main_app.get('/')
# the function you see in here can be seen in the swapper only
def get_homepage():
    return {"status": 'The application is up and working!  🚀 ',
            "settings": settings}


# I want to link some routers in here, with some prefix
main_app.include_router(homepage_router)
main_app.include_router(testing_router)
main_app.include_router(postgres_router)


