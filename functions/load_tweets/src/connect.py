import tweepy as tw
from google.cloud import bigquery as bq
from io import open
import os
import logging
import json
import sys
__author__: 'Camilo Lopez Ruiz'
__version__: '1.0'


class Connections:
    """
    this is the class for connect to different sources
    """
    def __init__(self, type_connect, file_params):
        """
        :param type_connect: This param specify type of connection
        :param file_params:  This param search the file for connect to api
        """
        self.type_connect = type_connect
        self.params = file_params
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(messages)s')
        self.logger = logging.getLogger()

    def get_connect(self):
        """
        This method get the connection and return this connection
        :return api: return api connection value
        """
        try:
            if self.type_connect == 'twitter':
                api = self.__twitter_connect()
                return api
            elif self.type_connect == 'bigquery':
                bq_client = self.__bigquery_connect()
                return bq_client
            else:
                self.logger.info('this type doesn\'t exists')
        except Exception as e:
            self.logger.error('An error has occurred info: ', e)

    def __twitter_connect(self):
        """
        This private method create connect for twitter with tweepy
        :return: Api connect to extract tweets
        """
        try:
            self.logger.info('the connection has began')
            with open(self.params) as file:
                self.params = json.load(file)
            consumer_key = self.params['consumer_key']
            consumer_secret = self.params['consumer_secret']
            access_token_key = self.params['access_token_key']
            access_token_secret = self.params['access_token_secret']
            auth = tw.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token_key, access_token_secret)
            api = tw.API(auth, wait_on_rate_limit=True,
                         wait_on_rate_limit_notify=True)
            consult = api.me()
            if len(str(consult.id)) > 0:
                return api
            else:
                self.logger.info('The connect fail try again or verify '
                                 'the params')
                sys.exit()
        except Exception as e:
            self.logger.error('An error has occurred when trying '
                              'connect info: ', e)
            sys.exit()

    def __bigquery_connect(self):
        try:
            self.logger.info('The connection for bigquery has begin')
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.params
            bq_client = bq.Client('sentimentalcovid19')
            return bq_client
        except Exception as e:
            self.logger.error('An error has occurred info: ', e)
