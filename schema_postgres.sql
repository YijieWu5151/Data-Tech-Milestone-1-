
CREATE TABLE PHRASES(
timestamp DATE NOT NULL,
tweet_id NUMERIC NOT NULL,
text varchar(1000) NOT NULL,
CONSTRAINT tweet_pkey PRIMARY KEY (tweet_id)
);
