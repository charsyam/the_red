class PostService:
    def write(self, conn, user_id, post_id, value):
        user_key = f"key:{user_id}"
        post_key = f"p:{user_id}"
        conn.zadd(user_key, {post_id: post_id})
        conn.hset(post_key, post_id, value)
        return {"post_id": post_id, "contents": value}

    def get(self, conn, user_id, post_id):
        post_key = f"p:{user_id}"
        post_raw = conn.hget(post_key, post_id)
        if not post_raw:
            return None

        return {"post_id": post_id, "contents": post_raw.decode('utf-8')}

    def list(self, conn, user_id, limit=10, last=-1):
        if last == -1:
            last = "+inf"

        key = f"key:{user_id}"
        print(key, last, limit)
        values = conn.zrevrangebyscore(key, last, "-inf", start=0, num=limit+1) 

        next_id = None
        length = len(values)
        if len(values) == limit+1:
            next_id = values[-1].decode('utf-8')

        results = [v.decode('utf-8') for v in values[:limit]]
        post_key = f"p:{user_id}"
        print(results)
        posts_raw = conn.hmget(post_key, results)

        datas = zip(results, posts_raw)
        unexisted_keys = []
        posts = []
        for data in datas:
            if data[1]:
                posts.append({"post_id": data[0], "contents": data[1]}) 
            else:
                unexisted_keys.append(data[0])
            
        if len(unexisted_keys) > 0:
            conn.delete(post_key, unexisted_keys)
 
        return (posts, next_id)
