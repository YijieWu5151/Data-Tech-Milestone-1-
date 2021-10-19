import requests
import os
import json
import time
import re
from datetime import datetime


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



def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth, stream=True)
    print(response.status_code)
    temp = []
    final = []

    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            #print(json.dumps(json_response, indent=4, sort_keys=True))
            json_data = json_response['data']
            json_temp = ({k: v for k, v in json_data.items() if k in ('created_at', 'text')})

            temp = json_temp.values()

            keys_values = json_temp.items()
            temp = {str(key): str(value) for key, value in keys_values}

            temp['text'] = ''.join(filter(lambda character:ord(character) < 0x100,temp['text']))

            final += temp.values()
            #time.sleep(1)

            #print(final)

            #myfile = open('text.txt', 'a')
            # myFile.write(str(final))
            # myFile.write('\n')  # adds a line between tweets
            # myFile.close()

        if len(final) > 1000:
            timestamp = final[::2]
            timestamp = [datetime.strptime(i,"%Y-%m-%dT%H:%M:%S.%fZ") for i in timestamp]
            timestamp = [i.strftime("%Y-%m-%d-%H-%M-%S") for i in timestamp]

            text = final[1::2]
            text = [" ".join(i.split()) for i in text]

            with open('tweets.txt', 'w') as log:

                for i in range(round(len(final)/2)):
                    log.write('{}\n'.format(str(timestamp[i]) + ', ' + str(text[i])))

            print("done")
            #time.sleep(60)
            break

    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text

            )
        )
    return final


def main():
    url = create_url()
    timeout = 0

    while True:
        connect_to_endpoint(url)
        timeout += 1

if __name__ == "__main__":
    main()

