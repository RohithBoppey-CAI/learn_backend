from pydantic_settings import BaseSettings, SettingsConfigDict

class ENVSettings(BaseSettings):
    # declare all the variables that are required for the application to run - MUST
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_SERVICE: str
    POSTGRES_DB: str
    POSTGRES_PORT: str
    POSTGRES_POOL_SIZE_STR: str
    POSTGRES_MAX_OVERFLOW_STR :str
    
    
    # can write additional settings from the env provided, such as converting from int to string right in here
    @property
    def POSTGRES_POOL_SIZE(self):
        return int(self.POSTGRES_POOL_SIZE_STR)
    
    @property
    def POSTGRES_MAX_OVERFLOW(self):
        return int(self.POSTGRES_MAX_OVERFLOW_STR)
    
    
    model_config = SettingsConfigDict(env_file='.env')
    
settings = ENVSettings()