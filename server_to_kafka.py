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

    ct.main()


if __name__ == "__main__":
    main()
