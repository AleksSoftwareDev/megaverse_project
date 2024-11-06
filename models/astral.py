
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class ObjectType(str, Enum):
    POLYANET = "polyanets"
    SOLOON = "soloons"
    COMETH = "comeths"


class CellType(str, Enum):
    POLYANET = "POLYANET"
    SOLOON = "SOLOON"
    COMETH = "COMETH"


class AstralObject(BaseModel):
    row: int
    column: int
    color: Optional[str] = None
    direction: Optional[str] = None


class AstralObject(BaseModel):
    row: int
    column: int
    color: Optional[str] = None
    direction: Optional[str] = None
