import server_postgres as sv
import create_table as ct

import time

def main():
    try:
        sv.main()
    except:
        print("Problem loading Tweets")
        time.sleep(900)
        sv.main()

    time.sleep(1)

    # import word_count_postgres as wcp
    # import vocabulary_size_postgres as vc
    #
    # wcp.word_table()
    #
    # wcp.word_count_in_current_minute('the')
    #
    # vc.distinct_words_in_minute()
    ct.main()




if __name__ == "__main__":
    main()
