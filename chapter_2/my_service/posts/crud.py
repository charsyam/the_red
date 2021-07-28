from sqlalchemy.orm import Session
from models import Posts


def create_url(db: Session, post_id: int, contents: str, url: str, scrap: str):
    post = Posts(post_id=post_id, contents=contents, url=url, scrap=scrap)
    db.add(post)
    db.commit()
    return post


def list(db: Session, from_post_id=-1, limit=20):
    q = db.query(Posts)
    if (from_post_id >= 0):
        q = q.where(Posts.post_id <= from_post_id)

    return q.order_by(Posts.uid.desc()).limit(limit).all()
