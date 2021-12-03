import pandas as pd
import argparse
import re


def count_words(search_words):
    # Open the file in read mode
    text = open("tweets.txt", "r")
    test_str = ""

    # Loop through each line of the file
    for line in text:
        # Remove the leading spaces and newline character
        line = line[20:]
        line = line.strip()
        line = re.sub('[^A-Za-z0-9]+', ' ', line)
        #print(line)
        line = re.sub('[^A-Za-z0-9]+', ' ', line)
        #print(line)
        # Convert the characters in line to
        # lowercase to avoid case mismatch
        line = line.lower()

        # Split the line into words
        words = line.split(" ")
        #append each line to the test string
        test_str = test_str + line

    res = test_str.count(search_words)

    print("The count of '", search_words, "'in tweets.txt:", res)

def main():

    parser = argparse.ArgumentParser(description='word count')
    parser.add_argument('--word', dest='words', type=str)

    args = parser.parse_args()

    count_words(args.words.lower())

if __name__ == "__main__":

    main()

