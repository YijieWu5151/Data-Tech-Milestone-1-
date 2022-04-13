import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--word',  type=str)
parser.add_argument('--day',  type=str)
parser.add_argument('--minute',  type=str)
parser.add_argument('--wordflag',type=str)
args = parser.parse_args()

input_word = args.word
input_time = args.day+' '+args.minute
word_flag = args.wordflag


import psycopg2
def connect_db():
    try:
        conn = psycopg2.connect(database='milestone2', user='gb760')
    except Exception as e:
        
        print("fail")
    else:
        return conn
    return None
def close_db_connection(conn):
    conn.commit()
    conn.close()

import pprint
def execute_sql(sql):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(sql)
    print(cur[0])
    ret = cur
    close_db_connection(conn)
    print(sql)
    return cur
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

def single_word_times_in_minute(word_count_table,minute_timestamp,single_word):
    ret = 0
    #if minute_timestamp not in word_count_table:
        #return 0        
    if (word_count_table[minute_timestamp].get(single_word,-1)!=-1):
        ret = word_count_table[minute_timestamp][single_word]
    # print("In "+minute_timestamp)
    # print(single_word+":"+str(ret))
    return ret


sql="select * from tweet"
tweet_table = execute_select(sql)
# print(tweet_table[0]['timestamp'])

word_count = {}
for row in tweet_table:
    minute_timestamp = row['timestamp']
    minute_timestamp = minute_timestamp[:16]
    tweet_text = row['text'].split()
    if (word_count.get(minute_timestamp,-1)==-1):
        word_count[minute_timestamp] = {}
    # print(tweet_text)
    for single_word in tweet_text:
        if (word_count[minute_timestamp].get(single_word,-1) == -1) :
            word_count[minute_timestamp][single_word] = 1
        else:
            word_count[minute_timestamp][single_word] = word_count[minute_timestamp][single_word] + 1
    if word_flag == 'multi':
        tweet_phrase_text = []
        for i in range(1,len(tweet_text)):
            phrase=tweet_text[i-1]+" "+tweet_text[i]
            tweet_phrase_text.append(phrase)
        for phrase in tweet_phrase_text:
            if (word_count[minute_timestamp].get(phrase,-1) == -1) :
                word_count[minute_timestamp][phrase] = 1
            else: 
                word_count[minute_timestamp][phrase] = word_count[minute_timestamp][phrase] + 1 


frequency = single_word_times_in_minute(word_count,input_time,input_word)
result = '\''+input_word+'\''+': ' +'frequency in '+input_time+' is '+str(frequency)
print(result)
