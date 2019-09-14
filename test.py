import psycopg2
import uuid
import json


#CREATE TABLE public."twiviews"(
#                        id text COLLATE pg_catalog."default" NOT NULL PRIMARY KEY,
#                        hashtag text COLLATE pg_catalog."default" NOT NULL,
#                        twiview text COLLATE pg_catalog."default" NOT NULL,
#                        sentiment text COLLATE pg_catalog."default" NOT NULL,
#                        signal double precision NOT NULL
#                       )

get_response = []

try:  # connect to PostgreSQL  
    conn = psycopg2.connect("dbname='postgres' host='localhost' user='postgres' password='TwiLi8'")
    # the SQL INSERT statement we will use
    # open a cursor to access data
    cur = conn.cursor()

    insert_sql = """
          INSERT INTO public."twiviews"(id, hashtag, twiview, sentiment, signal)
          VALUES (%(twiid)s, %(hashtag)s, %(twiview)s, %(sentiment)s, %(signal)s);         
          """
    read_sql = '''
    
     SELECT * FROM twiviews where hashtag = %(hashtag)s ORDER BY hashtag;
    
    '''

    #query_params_list = {'twiid': id, 'hashtag': "#O'Reilly", 'twiview': "twiview", 'sentiment': "positive", 'signal': 0.4}


    cur.execute(read_sql, {'hashtag': "#O'Reilly"})

    row = cur.fetchone()

    json_doc = { 'id': row[0],
                 'hashtag' : row[1],
                 'twiview' : row[2],
                 'sentiment' : row[3],
                 'signal' : row[4]
    }

    print(json.dumps(json_doc))
    while row is not None:
        get_response.append(json.dumps(row))
        row = cur.fetchone( )

    cur.close( )

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
