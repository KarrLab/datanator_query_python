"""For querying uniprot.org using uniprot API
(https://www.uniprot.org/help/api_queries)
"""
import requests
from bs4 import BeautifulSoup
import re


class QueryUniprotOrg:

    def __init__(self, api='https://www.uniprot.org/uniprot/?'):
        """Init
        
        Args:
            url (:obj:`int`, optional): API url.
        """
        self.api = api

    def get_kegg_ortholog(self, query, columns='database(KO)', include='yes', compress='no',
                          limit=1, offset=0):
        """Get kegg ortholog information using query message.

        Args:
            query (:obj:`str`): Query message.
            columns (:obj:`str`, optional): comma-separated list of column names. Defaults to 'id'.
            include (:obj:`str`, optional): See description in link. Defaults to 'yes'.
            compress (:obj:`str`, optional): Return results gzipped. Defaults to 'no'.
            limit (:obj:`int`, optional): Max number of results to return. Defaults to 1.
            offset (:obj:`int`, optional): Offset of the first result. Defaults to 0.
        """
        suffix = 'query={}&sort=score&columns={}&format={}&include={}&compress={}&limit={}&offset={}'.format(
                  query, columns, 'html', include, compress, limit, offset)
        url = self.api + suffix
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        rx = re.compile(".*dbget-bin.*")
        result = soup.find_all(href=rx)
        if result != []:
            return result[0].get_text()
        else:
            return None