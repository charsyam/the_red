from sqlalchemy.orm import Session
from models import Posts


def create_post(user_id: int, post_id: int, contents: str, url: str, scrap: str):
    return Posts(user_id=user_id, post_id=post_id, contents=contents, url=url, scrap=scrap)


def add(db: Session, user_id: int, post_id: int, contents: str, url: str, scrap: str):
    post = create_post(user_id=user_id, post_id=post_id, contents=contents, url=url, scrap=scrap)
    db.add(post)
    db.commit()
    return post


def list(db: Session, user_id, from_post_id=-1, limit=10):
    q = db.query(Posts).where(Posts.user_id == user_id)
    if (from_post_id >= 0):
        q = q.where(Posts.post_id <= from_post_id) 

    return q.order_by(Posts.uid.desc()).limit(limit).all()

def posts(db: Session, ids):
    return db.query(Posts).filter(Posts.post_id.in_(ids)).all()
