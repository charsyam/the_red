from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from database import Base


class Url(Base):
    __tablename__ = "url"

    uid = Column(Integer, primary_key=True, index=True)
    url = Column(String(256))
