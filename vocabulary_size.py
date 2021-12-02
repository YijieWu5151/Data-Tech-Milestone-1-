
import pandas as pd
import argparse
import re


def count_words():
    # Open the file in read mode
    text = open("tweets.txt", "r")
    # Create an empty dictionary
    d = dict()

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
        # Iterate over each word in line
        for word in words:
            # Check if the word is already in dictionary
            if word in d:
                # Increment count of word by 1
                d[word] = d[word] + 1
            else:
                # Add the word to dictionary with count 1
                d[word] = 1


    print("The number of unique words in tweets.txt =",len(d))

def main():
    count_words()

if __name__ == "__main__":

    main()
