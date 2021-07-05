from pydantic import BaseModel


class CodeEntity(BaseModel):
    code: int


class CountryEntity(CodeEntity):
    ip: str
    country: str


class CityEntity(CountryEntity):
    city: str
    latitude: float
    longitude: float
