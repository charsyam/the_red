from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Text, BigInteger
from sqlalchemy.orm import relationship

from database import Base


class Posts(Base):
    __tablename__ = "posts"
    uid = Column(BigInteger, primary_key=True, index=True)
    user_id = Column(BigInteger, index=True)
    post_id = Column(BigInteger, unique=True, index=True)
    contents = Column(Text)
    scrap = Column(Text)
    url = Column(String(1024))
