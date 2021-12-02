import requests
import os
import json
import time
import re
import copy as copy
from datetime import datetime
from dataclasses import dataclass
import argparse



os.environ['BEARER_TOKEN'] = 'AAAAAAAAAAAAAAAAAAAAAGakUQEAAAAAucI%2FuyC5RvtNok3cESZ93ITkRxQ%3DegMxLb1Q1A67ZK5AJJcghTtmyOryBWouBG6TSpLAjDWRSDI3CQ'
#print(os.environ)
# To set your environment variables in your terminal run the following line:
#'BEARER_TOKEN'= AAAAAAAAAAAAAAAAAAAAAGakUQEAAAAAucI%2FuyC5RvtNok3cESZ93ITkRxQ%3DegMxLb1Q1A67ZK5AJJcghTtmyOryBWouBG6TSpLAjDWRSDI3CQ
bearer_token = os.environ.get("BEARER_TOKEN")


def create_url():
    #return "https://api.twitter.com/2/tweets/sample/stream"
    return "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=created_at"




def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r

def transform_json(twitter_jsons):
    """
    Takes list of Json string and converts it to [{"timestamp": <datetime>, "content": <content>}]
    """
    final = []
    temp = []
    for twitter_json in twitter_jsons:
        json_response = json.loads(twitter_json)
        #print(json.dumps(json_response, indent=4, sort_keys=True))
        #print(json.dumps(json_response))
        json_data = json_response['data']
        json_temp = ({k: v for k, v in json_data.items() if k in ('created_at', 'text')})

        temp = json_temp.values()

        keys_values = json_temp.items()
        temp = {str(key): str(value) for key, value in keys_values}

        temp['text'] = ''.join(filter(lambda character:ord(character) < 0x100,temp['text']))
        tweet = {}
        tweet['timestamp'] = datetime.strptime(temp['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
        tweet['content'] = ' '.join(temp['text'].split())
        final.append(tweet)
        #print('tweet: timestamp:', tweet['timestamp'], ' content: ', tweet['content'])
    return final

def write_transformed_tweets_to_file(transformed_tweets):
    """
    Converts list of tweets in  [{"timestamp": <datetime>, "content": <content>}] format to tweets.txt in "<YYYY-MM-DD-HH-MM-SS>, <tweet text>"
    """
    with open('tweets.txt', 'a') as log:
        for transformed_tweet in transformed_tweets:
            num_written = log.write('{}\n'.format(str(transformed_tweet['timestamp'].strftime("%Y-%m-%d-%H-%M-%S")) + ', ' + str(transformed_tweet['content'])))
            if num_written == 0:
                raise Exception(
                    "Cannot write to file. Disk full?")



def parse_from_twitter_stream(url):
    """
    Transforms tweets from twitter sample stream into [{"timestamp": <datetime>, "content": <content>}]
    """
    response = requests.request("GET", url, auth=bearer_oauth, stream=True)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text

            )
        )

    twitter_jsons = []
    i = 0
    for response_line in response.iter_lines():
        if response_line:
            twitter_jsons.append(response_line)
            i += 1
        if i > 100:
            break;
    result =  transform_json(twitter_jsons)
    print("done")
    return result



def main():
    url = create_url()
    timeout = 0

    parser = argparse.ArgumentParser(description='Transform twitter sample stream')
    parser.add_argument('--filename', dest='file', type=argparse.FileType('r'))
    args = parser.parse_args()
    print(args.file)
    transformed_json = []
    while True:
        print("Looping")
        if args.file:
            jsons = []
            for line in args.file:
                jsons.append(line)
            args.file.seek(0)
            transformed_json = transform_json(jsons)
        else:
            try:
                transformed_json = parse_from_twitter_stream(url)
            except Exception:
                print("Error accessing twitter api. Sleeping 60 seconds and trying again")
                time.sleep(60)
                continue
        try:
            write_transformed_tweets_to_file(transformed_json)
        except Exception:
            print("Error writing tweets to file.")
        time.sleep(10)

if __name__ == "__main__":
    main()

