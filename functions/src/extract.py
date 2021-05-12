from connect import Connections
import tweepy as tw


class Extractions(Connections):

    def __init__(self, terms_search, file_params=None):
        self.file_params = '../config/twitter_key.json'
        self.terms_search = terms_search

    def twitter_extract(self):
        Connections.__init__(self, 'twitter', self.file_params)
        api = self.get_connect()
        tweets = tw.Cursor(api.search, q=self.terms_search,
                           tweet_mode='extended').items(10)
        for t in tweets:
            print(t._json)

if __name__ == '__main__':
    ext = Extractions('#YoMeVacuno OR #Vacuna')
    ext.twitter_extract()


