from psycopg2._psycopg import cursor

import psycopg2
from psycopg2 import Error

connection = None


def create_tables():
    # provide sql statements

    create_twiviews_table_query = '''
        
        CREATE TABLE public."twiviews"(
                        id text COLLATE pg_catalog."default" NOT NULL PRIMARY KEY, 
                        hashtag text COLLATE pg_catalog."default" NOT NULL,
                        twiview text COLLATE pg_catalog."default" NOT NULL,
                        sentiment text COLLATE pg_catalog."default" NOT NULL,
                        signal double precision NOT NULL
                        );
        
        '''

    create_checkpoint_table_query = '''
    
    CREATE TABLE public."checkpoint"(since_id NUMERIC not null primary key);
    
    '''


    try:

        connection = get_conn_rds()

        cur = connection.cursor()

        cur.execute(create_twiviews_table_query)
        cur.execute(create_checkpoint_table_query)
        connection.commit()

        print("twiviews, checkpoint Tables created successfully in rvwdbpg")

        cur.close()
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

def get_conn_rds():

    try:
        # connect to the postgresql database
        print("Connecting to the PostgreSQL database...")

        connection = psycopg2.connect(user = "postgres",
                                  password = "LKdB5SuyDYXEEBpQ6ph6",
                                  #host = "rvwdbpg.cpn2q9h6h0ua.us-east-1.rds.amazonaws.com", # --rvdb
                                  host = "rvwdbpg.cwslyap2b0lw.us-east-1.rds.amazonaws.com",  # --nvdb
                                   port = "5432",
                                  database = "postgres")

        return connection

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)




if __name__ == '__main__':
        create_tables()

