---
title: "R Notebook"
author: 99452
date: 12/6/2021
output: html_document
---
words(tweet_id[PK], timestamp, text)
phrases(word_id[PK], timestamp, tweet_id, word, phrase_yn)

In milestone2 assignment we need to create a datawarehouse to store the information we extracted from twitter.
The tweet stream we got is in format like "2021-10-19-01-36-40, Um mimo pra nessa segunda ", so in the first table, we could make three columns: tweet_id, time (accurate to second) and text content, and we set the primary key on the 'tweet_id'.
The second table is the phrases table, we set primary key on the 'word_id' in order to distinguish different words and phrases, and the 'word' column is used to store both words and phrases.
Plus, in 'phrase_yn' column we create a binary indicator variable which can indicate whether tweet in 'word' column is a phrase or not. If the value for phrase_yn is 1 , that means the tweet is a phrase, and o means the tweet is a single word.

