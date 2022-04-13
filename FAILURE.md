# FAILURE (milestone 1D)

- Lose access to internet. This results in http GET requests failing. Code raises exception and sleeps for 60 seconds.
- Tweet has no content after phrase cleanup. Code ignores tweet.
- Text isn't in string format. Decode as utf 8. If text is still unreadable encoding, will be empty and therefore dropped.
- Tweet json in twitter sample api response doesn't exist, code moves on to next 'iter_line'.
- Invalid file name input for json formatted tweets. Code will give command line error.
- Run out of disk space. This results in failure to write tweets to file. Code handles exception by continued execution.

