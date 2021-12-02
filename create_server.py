import psycopg2
from server_postgres import execute_sql
from server_postgres import close_db_connection

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

#Creating a table
# sql = "drop table if exists words;"
# execute_sql(sql)
# sql = "CREATE TABLE WORDS(timestamp TIMESTAMP NOT NULL, tweet_id NUMERIC NOT NULL, word_id NUMERIC NOT NULL,word varchar(1000) NOT NULL," \
#       "CONSTRAINT word_pkey PRIMARY KEY (word_id));"
# execute_sql(sql)
#
# print("Table created successfully........")
#
#
# #Preparing query to create a database
# conn = connect_db()
# cur = conn.cursor()
#
# sql="select * from phrases"
# tweet_table = execute_select(sql)
# # print(ret)
# print(tweet_table[0]['timestamp'])
# j = 0 #Counter for PK in table
#
# conn = connect_db()
# cur = conn.cursor()
# for row in tweet_table:
#    minute_timestamp = str(row['timestamp'])
#    minute_timestamp = minute_timestamp[:16]
#    tweet_id = str(row['tweet_id'])
#    #minute_timestamp = 1
#    tweet_text = row['text'].split()
#    j += 100
#
#    for i in tweet_text:
#       #print(tweet_id)
#
#       cur.execute("""insert into words(timestamp, tweet_id, word_id, word) values(%s,%s,%s,%s); """, (minute_timestamp, tweet_id, j, i))
#
#       sql = ("insert into words(timestamp, tweet_id, word_id, word) " + "values(%s,%s,%s,%s)" % (minute_timestamp,tweet_id, j, i) + ";")
#       #print(sql)
#
#       #execute_sql(sql)
#       j += 1
#
# close_db_connection(conn)


def word_count_in_minute( minute_timestamp, single_word):
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(*) from words
   where timestamp >=  %s
   and timestamp <= timestamp %s + interval '1 minutes'
   and word LIKE %s """
   cur.execute(sql , (minute_timestamp, minute_timestamp, single_word))
   list_header = [row[0] for row in cur.description]
   list_result = [[str(item) for item in row] for row in cur.fetchall()]
   res = [dict(zip(list_header, row)) for row in list_result]

   close_db_connection(conn)

   print(res)
   return res

word_count_in_minute('2021-12-02 20:42:00', 'year')


def distinct_words_in_minute( minute_timestamp):
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(distinct word) from words
      where timestamp >=  %s
      and timestamp <= timestamp %s + interval '1 minutes' """
   cur.execute(sql , (minute_timestamp, minute_timestamp))
   list_header = [row[0] for row in cur.description]
   list_result = [[str(item) for item in row] for row in cur.fetchall()]
   res = [dict(zip(list_header, row)) for row in list_result]

   close_db_connection(conn)

   print(res)
   return res

distinct_words_in_minute('2021-12-02 20:42:00')