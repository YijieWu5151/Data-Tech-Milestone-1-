import psycopg2

conn = psycopg2.connect(database='milestone2', user='gb760')
# create a cursor
cur = conn.cursor()

# execute a SQL command
query = """
CREATE TABLE PHRASES(
timestamp DATE NOT NULL,
tweet_id NUMERIC NOT NULL,
text varchar(1000) NOT NULL,
CONSTRAINT tweet_pkey PRIMARY KEY (tweet_id)
);
"""
cur.execute(query)
conn.commit()

