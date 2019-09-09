from psycopg2._psycopg import cursor

import psycopg2
from psycopg2 import Error

connection = None


def create_tables():
    # provide sql statements

    create_table_query = '''
        CREATE TABLE twiviews
        (   id SERIAL PRIMARY KEY,
            hashtag VARCHAR(255) NOT NULL,
            twiview VARCHAR(255) NOT NULL,
            sentiment VARCHAR(255) NOT NULL,
            signal Decimal NOT NULL
        );'''

    try:

        connection = get_conn_rds()

        cur = connection.cursor()

        cur.execute(create_table_query)
        connection.commit()

        print("twiviews Table created successfully in rvwdbpg")

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
                                  host = "rvwdbpg.cpn2q9h6h0ua.us-east-1.rds.amazonaws.com",
                                  port = "5432",
                                  database = "postgres")

        return connection

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)




if __name__ == '__main__':
        create_tables()

