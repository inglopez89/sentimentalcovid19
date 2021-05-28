from .connect import Connections
import tweepy as tw
import pandas as pd
import logging
__author__: 'Camilo Lopez Ruiz'
__version__: '1.0'


class Extractions():
    """
    This Class is for extract the information
    """
    def __init__(self):
        """
        for authenticate if is twitter see the structure file, for bigquery
        specify the source.
        """
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(message)s')
        self.logger = logging.getLogger()
        self.file_params = '../load_tweets/config/twitter_key.json'
        self.connections = Connections(self, 'twitter', self.file_params)

    def twitter_extract(self, terms_search, items):
        """
        This method is for extract the tweets with terms defined for user
        :param items: number of items to return in the consult
        :param terms_search: this is the word or hastag to search
        :return: dataframe object
        """
        try:
            self.logger.info('The twitter extract has begin')
            api = self.connections.get_connect(self)
            tweets = tw.Cursor(api.search, q=terms_search,
                               tweet_mode='extended').items(items)
            df = pd.DataFrame({})
            for t in tweets:
                user = dict(t._json['user'])
                df_temp = pd.DataFrame({
                    'tweet_id': pd.Series(t._json['id']).astype('string'),
                    'create_at': pd.Series(
                        t._json['created_at']).astype('string'),
                    'message': pd.Series(t._json['full_text']).astype('string'),
                    'location': pd.Series(user['location']).astype('string'),
                    'favorite_count': pd.Series(
                        t._json['favorite_count']).astype('string'),
                    'retweet_count': pd.Series(
                        t._json['retweet_count']).astype('string')
                }, dtype='string')
                df = pd.concat([df, df_temp])
            return df
        except Exception as e:
            self.logger.error('An error has occurred extract twitter info: ', e)
