from connect import Connections
import tweepy as tw
import pandas as pd
__author__: 'Camilo Lopez Ruiz'
__version__: '1.0'


class Extractions(Connections):
    """
    This Class is for extract the information
    """
    def __init__(self, file_params):
        """
        :param file_params:is the number of file with contain the information
        for authenticate if is twitter see the structure file, for bigquery
        specify the source.
        """
        self.file_params = file_params

    def twitter_extract(self, terms_search, items):
        """
        This method is for extract the tweets with terms defined for user
        :param items: number of items to return in the consult
        :return: dataframe object
        """
        try:
            Connections.__init__(self, 'twitter', self.file_params)
            api = self.get_connect()
            tweets = tw.Cursor(api.search, q=terms_search,
                               tweet_mode='extended').items(items)
            df = pd.DataFrame(columns={'tweet_id', 'create_at', 'message',
                                       'favorite_count', 'retweet_count',
                                       'location'})
            for t in tweets:
                df['tweet_id'] = pd.Series(t._json['id'])
                df['create_at'] = pd.Series(t._json['created_at'])
                df['message'] = pd.Series(t._json['full_text'])
                user = dict(t._json['user'])
                df['location'] = pd.Series(user['location'])
                df['favorite_count'] = pd.Series(t._json['favorite_count'])
                df['retweet_count'] = pd.Series(t._json['retweet_count'])
            return df
        except Exception as e:
            self.logger.error('An error has occurred info: ', e)


if __name__ == '__main__':
    ext = Extractions('../config/twitter_key.json')
    ext.twitter_extract('put your word search or #hastag', 10)
