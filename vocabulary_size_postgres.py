import psycopg2
from server_postgres import execute_sql
from server_postgres import close_db_connection
import server_postgres as sp
import time

def connect_db():
   try:
      conn = psycopg2.connect(database='Milestone2', user='postgres',
                              password='123', host='127.0.0.1', port=5432)
   except Exception as e:

      print("fail")
   else:
      return conn
   return None


def execute_select(sql):
   conn = connect_db()
   cur = conn.cursor()
   cur.execute(sql)
   list_header = [row[0] for row in cur.description]
   list_result = [[str(item) for item in row] for row in cur.fetchall()]
   res = [dict(zip(list_header, row)) for row in list_result]
   # print(list_header)
   # print(list_result)
   # print(res)
   return res

def word_table():
   #Creating a table
   sql = "drop table if exists words;"
   execute_sql(sql)
   sql = "CREATE TABLE WORDS(word_id SERIAL PRIMARY KEY,timestamp TIMESTAMP NOT NULL, tweet_id NUMERIC NOT NULL," \
         " word varchar(1000) NOT NULL, phrase_yn NUMERIC NOT NULL);"

   execute_sql(sql)

   print("Table created successfully........")


   #Preparing query to create a database
   conn = connect_db()
   cur = conn.cursor()

   sql="select * from phrases"
   tweet_table = execute_select(sql)
   # print(ret)
   #print(tweet_table[0]['timestamp'])
   j= 0

   conn = connect_db()
   cur = conn.cursor()
   for row in tweet_table:
      minute_timestamp = str(row['timestamp'])
      #minute_timestamp = minute_timestamp[:16]
      tweet_id = str(row['tweet_id'])
      #row['text'] = row['text'].replace("'",'')
      #minute_timestamp = 1
      tweet_text = row['text'].split()
      phrases = [tweet_text[i] + ' ' + tweet_text[i+1] for i in range(len(tweet_text)-1)]

      j += 100

      for i in tweet_text:
         #print(tweet_id)

         cur.execute("""insert into words(timestamp, tweet_id, word,  phrase_yn) values(%s,%s,%s,0); """, (minute_timestamp, tweet_id, i))

         sql = ("insert into words(timestamp, tweet_id, word_id, word,  phrase_yn) " + "values(%s,%s,%s,0)" % (minute_timestamp,tweet_id, i) + ";")
         #print(sql)

         #execute_sql(sql)
         j += 1

      for k in phrases:

         cur.execute("""insert into words(timestamp, tweet_id, word, phrase_yn) values(%s,%s,%s,1); """,
                     (minute_timestamp, tweet_id, k))

         sql = ("insert into words(timestamp, tweet_id, word,  phrase_yn) " + "values(%s,%s,%s,1)" % (
         minute_timestamp, tweet_id, k) + ";")
         # print(sql)

         # execute_sql(sql)
         j += 1


   close_db_connection(conn)



def distinct_words_in_minute():
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(distinct word)
         from words
         where
         timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' 
         and timestamp <= localtimestamp + interval '6 hours'
         and phrase_yn = 0 """
   cur.execute(sql)
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('distinct_words_in_minute:')
   print(int(res[1]))
   return res

distinct_words_in_minute()
