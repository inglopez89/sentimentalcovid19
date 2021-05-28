from .extract import Extractions
from .load import Load
import logging
import sys
__author__: 'Camilo Lopez Ruiz'
__version__: '1.0'


class LoadCovid:
    """
    class to extract tweets and load
    """

    def __init__(self):
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(message)s')
        self.logger = logging.getLogger()
        self.project_id = 'sentimentalcovid19'
        self.extract_tweets = Extractions()
        self.load_bigquery = Load()

    def load_tweets_covid(self, terms, items):
        """
        This method make a extraction and load to bigquery table the tweets
        extracted.
        :return:
        """
        dataframe = self.extract_tweets.twitter_extract(terms, items)
        if dataframe is None:
            self.logger.info('Dataframe is empty')
            sys.exit()
        else:
            self.load_bigquery.load_bigquery(dataframe, self.project_id,
                                             'stagging_data', 'tweet')
