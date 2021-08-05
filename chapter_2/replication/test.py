import pymysql
import time


HOST = "database-1.cluster-cti5mc0i9dvk.ap-northeast-2.rds.amazonaws.com"
USER = "the_red"
PASSWORD = "the_red"
DB = "the_red"

connection = pymysql.connect(host=HOST,
                             user=USER,
                             password=PASSWORD,
                             database=DB,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


while True:
    with connection.cursor() as cursor:
            # Create a new record
        sql = "INSERT INTO `test` VALUES (1)"
        cursor.execute(sql)

    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT MAX(uid) from test"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(result)

    time.sleep(3)
