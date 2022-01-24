from src.load_covid import LoadCovid


def main():
    execute = LoadCovid()
    execute.load_tweets_covid('#YoMeVacuno OR #Vacuna', 5000)

if __name__ == '__main__':
    main()
