# Validate all the API requests from in here

from pydantic import BaseModel

class DannyDinerRequest(BaseModel):
    question_number: int