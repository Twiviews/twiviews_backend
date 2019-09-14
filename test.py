import psycopg2
import uuid


#CREATE TABLE public."twiviews"(
#                        id text COLLATE pg_catalog."default" NOT NULL PRIMARY KEY,
#                        hashtag text COLLATE pg_catalog."default" NOT NULL,
#                        twiview text COLLATE pg_catalog."default" NOT NULL,
#                        sentiment text COLLATE pg_catalog."default" NOT NULL,
#                        signal double precision NOT NULL
#                       )

try:  # connect to PostgreSQL  
    conn = psycopg2.connect("dbname='postgres' host='localhost' user='postgres' password='TwiLi8'")
    # the SQL INSERT statement we will use
    # open a cursor to access data
    cur = conn.cursor()

    sql = """
          INSERT INTO public."twiviews"(id, hashtag, twiview, sentiment, signal)
          VALUES (%(twiid)s, %(hashtag)s, %(twiview)s, %(sentiment)s, %(signal)s);
          """
    query_params_list = {'twiid': id, 'hashtag': "#O'Reilly", 'twiview': "twiview", 'sentiment': "positive", 'signal': 0.4}

    cur.execute(sql, query_params_list)
    # cur.execute(
    # """
    # INSERT INTO public."twiviews"(id, hashtag, twiview, sentiment, signal)
    #     VALUES (%(twiid)s, %(hashtag)s, %(twiview)s, %(sentiment)s, %(signal)s);
    #      """,
    # {'twiid': str(uuid.uuid4()), 'hashtag': "#O'Reilly", 'twiview': "twiview", 'sentiment': "positive", 'signal': 0.4})

    conn.commit()
    cur.close()
    conn.close()
    print("Successfully wrote data to the database")
except Exception as ex:
    print(ex)
