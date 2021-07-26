from sqlalchemy.orm import Session
from models import Url


def create_url(db: Session, url):
    db_url = Url(url=url)
    db.add(db_url)
    db.commit()
    db.refresh(db_url)
    return db_url


def list(db: Session):
    return db.query(Url).order_by(Url.uid.desc())
