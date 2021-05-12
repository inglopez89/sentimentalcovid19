import tweepy as tw
from io import open
import logging
import json


class Connections:

    def __init__(self, type_connect, file_params):
        self.type_connect = type_connect
        self.params = file_params
        logging.basicConfig(format='%(acstime)s : %(levelname)s : (messages)s')
        self.logger = logging.getLogger()

    def get_connect(self):
        try:
            if self.type_connect == 'twitter':
                api = self.__twitter_connect()
                return api
            elif self.type_connect == 'bigquery':
                print('work on it')
            else:
                self.logger.info('this type does\'nt exists')
        except Exception as e:
            self.logger.error('An error has occurred info: ', e)

    def __twitter_connect(self):
        """
        This method create connect for twitter with tweepy
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
        except Exception as e:
            self.logger.error('An error has occurred when trying connect'
                              'to twitter info: ', e)
