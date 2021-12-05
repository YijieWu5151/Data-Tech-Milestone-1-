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
      row['text'] = row['text'].replace("'",'')
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




def word_frequency_in_minute( single_word):
   conn = connect_db()
   cur = conn.cursor()
   sql = """with occur as(select
            count(word) Occurence
			from words
            where timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' 
            and timestamp <= localtimestamp + interval '6 hours'
            and word LIKE %s)

            ,total as(	select count(*) total_words
            from words
            where phrase_yn = 0)
            
            select round(Occurence::decimal/total_words, 8) as Frequency
            from occur, total"""
   cur.execute(sql , ( single_word,))
   list_header = [row[0] for row in cur.description]
   list_result = [[str(item) for item in row] for row in cur.fetchall()]
   res = [dict(zip(list_header, row)) for row in list_result]

   close_db_connection(conn)

   print('word_frequency_in_minute:')
   print(int(res[1]))

   return res


#word_frequency_in_minute('the')


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
   list_header = [row[0] for row in cur.description]
   list_result = [[str(item) for item in row] for row in cur.fetchall()]
   res = [dict(zip(list_header, row)) for row in list_result]

   close_db_connection(conn)

   print('distinct_words_in_minute:')
   print(int(res[1]))
   return res

#distinct_words_in_minute()

def phrases_in_current_minute():
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(word)
         from words
         where
         timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' 
         and timestamp <= localtimestamp + interval '6 hours'
         """
   cur.execute(sql)
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('phrases_in_current_minute:')
   print(int(res[1]))
   return res

#phrases_in_current_minute()

def phrases_in_prior_minute():
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(word)
         from words
         where
         timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' - interval '1 minutes'
         and timestamp < date_trunc('minute', localtimestamp) + interval '6 hours' 
         """
   cur.execute(sql)
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('phrases_in_prior_minute:')
   print(int(res[1]))
   return res

#phrases_in_prior_minute()

def distinct_phrases_in_current_minute():
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(distinct word)
         from words
         where
         timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' 
         and timestamp <= localtimestamp + interval '6 hours'
         """
   cur.execute(sql)
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('distinct_phrases_in_current_minute:')
   print(int(res[1]))
   return res

#distinct_phrases_in_current_minute()

def distinct_phrases_in_prior_minute():
   conn = connect_db()
   cur = conn.cursor()
   sql = """select count(distinct word)
         from words
         where
         timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' - interval '1 minutes'
         and timestamp < date_trunc('minute', localtimestamp) + interval '6 hours'
         """
   cur.execute(sql)
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('distinct_phrases_in_prior_minute:')
   print(int(res[1]))
   return res

#distinct_phrases_in_prior_minute()

def word_count_in_current_minute(single_word):
   conn = connect_db()
   cur = conn.cursor()
   sql = """select
            count(word) from words
            where
            timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' 
            and timestamp <= localtimestamp + interval '6 hours' 
            and word LIKE %s """
   cur.execute(sql , (single_word,))
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('word_count_in_current_minute:')
   print(int(res[1]))
   return res

#word_count_in_current_minute('year')

def word_count_in_prior_minute(single_word):
   conn = connect_db()
   cur = conn.cursor()
   sql = """select
            count(word) from words
            where
            timestamp >= date_trunc('minute', localtimestamp) + interval '6 hours' - interval '1 minutes'
            and timestamp < date_trunc('minute', localtimestamp) + interval '6 hours'
            and word LIKE %s """
   cur.execute(sql , (single_word,))
   list_header = [row[0] for row in cur.description][0]
   list_result = [[str(item) for item in row] for row in cur.fetchall()][0][0]
   res = [list_header, list_result]

   close_db_connection(conn)

   print('word_count_in_prior_minute:')
   print(int(res[1]))
   return res

#word_count_in_prior_minute('year')

import math
def trendiness_calc(phrase):
    prob_current_minute = (1 + int(word_count_in_current_minute(phrase)[1]))/(int(phrases_in_current_minute()[1]) + int(distinct_phrases_in_current_minute()[1]))
    prob_prior_minute = (1 + int(word_count_in_prior_minute(phrase)[1]))/ (int(phrases_in_prior_minute()[1]) + int(distinct_phrases_in_prior_minute()[1]))

    trendiness = math.log10(prob_current_minute) - math.log10(prob_prior_minute)

    print('The Trendines Score of ' + str(phrase) + ' is ' + str(trendiness))

#trendiness_calc('the')

# select
# count(distinct
# word) from words
#
# select
# count(distinct
# word) from words
#
# where
# timestamp >= date_trunc('minute', localtimestamp) + interval '5 hours'
# and timestamp <= localtimestamp + interval '6 hours' ;
# select
# date_trunc('minute', localtimestamp)
# union
# select
# date_trunc('second', localtimestamp)

def main():
   word_table()

   try:
      trendiness_calc('the')
   except ZeroDivisionError:
      print("Not enough tweets in the current minute")
      #sp.main(500)

   try:
      trendiness_calc('like')
   except ZeroDivisionError:
      print("Not enough tweets in the current minute")
      #sp.main(500)

   try:
      trendiness_calc('love it')
   except ZeroDivisionError:
      print("Not enough tweets in the current minute")

   try:
      trendiness_calc('good')
   except ZeroDivisionError:
      print("Not enough tweets in the current minute")

if __name__ == "__main__":
    main()