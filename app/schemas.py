from datetime import date
from pydantic import BaseModel

class RangeParams(BaseModel):
    start: date
    end: date
