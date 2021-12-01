import Twiiter_stream as ts
import pandas as pd
import argparse
import re
# parser = argparse.ArgumentParser(description='Create Twitter.txt file based on either a Twitter sample stream'
#                                              'or a local JSON file')
# parser.add_argument('Stream', metavar='T', type=str,
#                     help='a Twitter Sample Stream object to be read in')
# parser.add_argument('--filename', dest='file_option', action='store_const',
#                     const=sum, default=max,
#                     help='read in JSON file (Twitter sample stream is default)')
#
# args = parser.parse_args()
# print(args.file_option(args.Stream))
#testing
def count_and_unique(test_sub):
    # Open the file in read mode
    text = open("tweets.txt", "r")
    test_sub = test_sub
    test_str = ""
    # Create an empty dictionary
    d = dict()

    # Loop through each line of the file
    for line in text:
        # Remove the leading spaces and newline character
        line = line[20:]
        line = line.strip()
        line = re.sub('[^A-Za-z0-9]+', ' ', line)
        #print(line)
        line = re.sub('[^A-Za-z0]+', ' ', line)
        #print(line)
        # Convert the characters in line to
        # lowercase to avoid case mismatch
        line = line.lower()

        # Split the line into words
        words = line.split(" ")
        #append each line to the test string
        test_str = test_str + line
        # Iterate over each word in line
        for word in words:
            # Check if the word is already in dictionary
            if word in d:
                # Increment count of word by 1
                d[word] = d[word] + 1
            else:
                # Add the word to dictionary with count 1
                d[word] = 1

    # Print the contents of dictionary
    #for key in sorted(d.keys()):
        #print(key, ":", d[key])

    res = test_str.count(test_sub)

    print("The count of '", test_sub, "'in tweets.txt:", res)

    print("The number of unique words in tweets.txt =",len(d))
def use_file(file):
    file + 1
    return file

def main():


    timeout = 0

    while True:
        url = ts.create_url()
        ts.connect_to_endpoint(url)
        test_sub = "new york"
        count_and_unique(test_sub)
        #     timeout += 1

if __name__ == "__main__":

    main()

