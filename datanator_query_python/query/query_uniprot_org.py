"""For querying uniprot.org using uniprot API
(https://www.uniprot.org/help/api_queries)
"""
import requests
from bs4 import BeautifulSoup
import re


class QueryUniprotOrg:

    def __init__(self, query, api='https://www.uniprot.org/uniprot/?', include='yes', compress='no',
                limit=1, offset=0):
        """Init
        
        Args:
            query (:obj:`str`): query message.
            url (:obj:`int`, optional): API url.
            include (:obj:`str`, optional): See description in link. Defaults to 'yes'.
            compress (:obj:`str`, optional): Return results gzipped. Defaults to 'no'.
            limit (:obj:`int`, optional): Max number of results to return. Defaults to 1.
            offset (:obj:`int`, optional): Offset of the first result. Defaults to 0.
        """
        columns = 'id,entry name,genes(PREFERRED),protein names,sequence,length,mass,ec,database(GeneID),reviewed,organism-id,database(KO),genes(ALTERNATIVE),genes(ORF),genes(OLN),database(EMBL),database(RefSeq),database(KEGG)'
        suffix = 'query={}&sort=score&columns={}format={}&include={}&compress={}&limit={}&offset={}'.format(
                  query, columns, 'html', include, compress, limit, offset)
        url = api + suffix
        response = requests.get(url)
        self.soup = BeautifulSoup(response.content, 'html.parser')

    def get_kegg_ortholog(self):
        """Get kegg ortholog information using query message.
        """
        rx = re.compile(".*dbget-bin.*")
        result = self.soup.find_all(href=rx)
        if result != []:
            return result[0].get_text()
        else:
            return None

    def get_uniprot_id(self):
        """Get uniprot id.
        """
        result = self.soup.find_all(class_='basket-item namespace-uniprot')
        if result != []:
            return result[0]['id'].split('_')[1]
        else:
            return None        