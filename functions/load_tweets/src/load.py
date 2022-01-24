import pandas_gbq as pbq
from .connect import Connections
import logging


class Load(Connections):
    """
    this class is for load dataframe of tweets extracted
    """
    def __init__(self, file_params):
        Connections.__init__(self, 'bigquery', file_params)
        logging.basicConfig(format='%(acstime)s : %(levelname)s : %(message)s')
        self.logger = logging.getLogger()

    def load_bigquery(self, df, project, dataset, table):
        """
        this method is for load tweets to bigquer schema or table
        :param df: Is a dataframe with differents fields
        :param project: is a name of project in Google Cloud
        :param dataset: is a name of dataset en in google cloud
        :param table: is a name of table en bigquery
        :return: it doesn't return
        """
        try:
            self.logger.info('Load bigquery has began')
            Connections.get_connect(self)
            pbq.to_gbq(df, dataset + '.' + table, project, if_exists='append')
        except Exception as e:
            self.logger.error('An error has occurred info : ', e)
