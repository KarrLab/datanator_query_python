"""For querying uniprot.org using uniprot API
(https://www.uniprot.org/help/api_queries)
"""
import requests
import beautifulsoup4


class QueryUniprotOrg:

    def __init__(self, api='https://www.uniprot.org/uniprot/?'):
        """Init
        
        Args:
            url (:obj:`int`, optional): API url.
        """
        self.api = api

    def get_kegg_ortholog(self, query, _format='html', columns='database(KO)', include='yes', compress='no',
                          limit=1, offset=0):
        """Get kegg ortholog information using query message.

        Args:
            query (:obj:`str`): Query message.
            _format (:obj:`str`, optional): Format in which to return results. Defaults to 'html'.
            columns (:obj:`str`, optional): comma-separated list of column names. Defaults to 'id'.
            include (:obj:`str`, optional): See description in link. Defaults to 'yes'.
            compress (:obj:`str`, optional): Return results gzipped. Defaults to 'no'.
            limit (:obj:`int`, optional): Max number of results to return. Defaults to 1.
            offset (:obj:`int`, optional): Offset of the first result. Defaults to 0.
        """
        suffix = 'query={}&sort=score&columns={}&format={}&include={}&compress={}&limit={}&offset={}'.format(
                  query, columns, _format, include, compress, limit, offset)
        url = self.api + suffix
        response = requests.get(url)
        return response.content