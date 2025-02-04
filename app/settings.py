from pydantic_settings import BaseSettings, SettingsConfigDict

class ENVSettings(BaseSettings):
    # declare all the variables that are required for the application to run - MUST
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_SERVICE: str
    POSTGRES_DB: str
    POSTGRES_PORT: str
    
    model_config = SettingsConfigDict(env_file='.env')
    
settings = ENVSettings()