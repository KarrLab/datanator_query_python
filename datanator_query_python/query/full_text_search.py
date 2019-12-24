from karr_lab_aws_manager.elasticsearch_kl import query_builder as es_query_builder
import numpy as np
import math
import json
import requests


class FTX(es_query_builder.QueryBuilder):

    def __init__(self, profile_name=None, credential_path=None,
                config_path=None, elastic_path=None,
                cache_dir=None, service_name='es', max_entries=float('inf'), verbose=False):
        super().__init__(profile_name=profile_name, credential_path=credential_path,
                config_path=config_path, elastic_path=elastic_path,
                cache_dir=cache_dir, service_name=service_name, max_entries=max_entries, verbose=verbose)

    def simple_query_string(self, query_message, index, **kwargs):
        ''' Perform simple_query_string in elasticsearch
            (https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/full-text/#simple-query-string)
            
            Args:
                query_message (:obj:`str`): simple string for querying
                index (:obj:`str`): comma separated string to indicate indices in which query will be done
                **size (:obj:`int`): number of hits to be returned
                **from_ (:obj:`int`): starting offset (default: 0)
                **scroll (:obj:`str`): specify how long a consistent view of the index should be maintained for scrolled search
                (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-scroll).
        '''
        body = self.build_simple_query_string_body(query_message, **kwargs)
        from_ = kwargs.get('from_', 0)
        size = kwargs.get('size', 10)
        es = self.build_es()
        r = es.search(index=index, body=json.dumps(body), from_=from_, size=size, explain=False)
        return r

    def bool_query(self, query_message, index, must=None, should=None, must_not=None, _filter=None, **kwargs):
        ''' Perform boolean query in elasticsearch
            (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html)
            
            Args:
                query_message (:obj:`str`): simple string for querying
                index (:obj:`str`): comma separated string to indicate indices in which query will be done.
                must (:obj:`list` or :obj:`dict`, optional): Body for must. Defaults to None.
                _filter (:obj:`list` or :obj:`dict`, optional): Body for filter. Defaults to None.
                should (:obj:`list` or :obj:`dict`, optional): Body for should. Defaults to None.
                must_not (:obj:`list` or :obj:`dict`, optional): Body for must_not. Defaults to None.
                **size (:obj:`int`): number of hits to be returned
                **from_ (:obj:`int`): starting offset (default: 0)
                **scroll (:obj:`str`): specify how long a consistent view of the index should be maintained for scrolled search
                (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-scroll).
        '''
        simple_str_query_body = self.build_simple_query_string_body(query_message, **kwargs)
        part_must = simple_str_query_body['query']
        if must is None:
            must = part_must
        elif isinstance(must, dict):
            must = [must].append(part_must)
        else:
            must.append(part_must)
        body = self.build_bool_query_body(must=must, should=should, _filter=_filter, must_not=must_not)
        from_ = kwargs.get('from_', 0)
        size = kwargs.get('size', 10)
        es = self.build_es()
        r = es.search(index=index, body=json.dumps(body), from_=from_, size=size, explain=False)
        return r

    def get_index_in_page(self, r, index):
        """Get indices in current hits page
        
        Args:
            r (:obj:`dict`): ftx search result
            index (:obj:`list`): list of string of indices.

        Return:
            (:obj:`dict`): obj of index hits {'index_0': [], 'index_1': []}
        """
        result = {}
        for item in index:
            result[item] = []
        hits = r['hits']['hits']
        if hits == []:
            return result
        else:
            for hit in hits:
                if hit['_index'] in index:
                    hit['_source']['_score'] = hit['_score']
                    result[hit['_index']].append(hit['_source'])
            return result

    def get_num_source(self, q, q_index, index, fields=['name', 'synonyms'], 
                        count=10, from_=0, batch_size=100):
        """Extract a count number of source (ecmdb, ymdb, metabolite_meta, etc) index
        from ftx search result
        
        Args:
            q (:obj:`str`): ftx query message
            q_index (:obj:`str`): comma separated string to indicate indices in which query will be done
            index (:obj:`set`): set of index of interest (source collections)
            fields (:obj:`list`, optional): list of fields to query. Defaults to ['name', 'synonyms']
            count (:obj:`int`, optional): number of records required. Defaults to 0.
            from_ (:obj:`int`, optional): page start. Defaults to 0.
            batch_size (:obj:`int`, optional): ftx query page size. Defaults to 100.

        Return:
            (:obj:`list`): list of hits of index
        """
        result = []
        r_0 = self.simple_query_string(q, q_index, from_=from_, size=batch_size, fields=fields)
        total = r_0['hits']['total']['value']
        if total < count:
            count = total
        from_list = np.arange(from_, total, batch_size).tolist()
        iteration = 0
        while len(result) < count:
            r = self.simple_query_string(q, q_index, from_=from_list[iteration], size=batch_size, fields=fields)
            hits = r['hits']['hits']
            for hit in hits:
                if len(result) >= count:
                    break
                elif hit['_index'] in index:
                    result.append(hit['_source'])
            iteration += 1
        return result

    def get_single_index_count(self, q, index, num, **kwargs):
        """Get single index up to num hits
        
        Args:
            q (:obj:`str`): query message
            index (:obj:`str`): index in which query will be performed
            num (:obj:`int`): number of hits needed

        Return:
            (:obj:`dict`): obj of index hits {'index': []}
        """
        result = {}
        result[index] = []
        body = self.build_simple_query_string_body(q, **kwargs)
        from_ = kwargs.get('from_', 0)
        r = self.build_es().search(index=index, body=body, size=num, from_=from_)
        hits = r['hits']['hits']
        if hits == []:
            result[index] = []
            return result
        else:
            for hit in hits:
                hit['_source']['_score'] = hit['_score']
                result[index].append(hit['_source'])
            return result

    def get_protein_ko_count(self, q, num, **kwargs):
        """Get protein index with different ko_number field for up to num hits.
        
        Args:
            q (:obj:`str`): query message.
            num (:obj:`int`): number of hits needed.
            **from_ (:obj:`int`): starting offset (default: 0).

        Return:
            (:obj:`dict`): obj of index hits {'index': []}
        """
        result = {}
        index = 'protein'
        must_not = {"bool": {
                        "must_not": {
                            "exists": {
                                "field": "ko_number"
                            }
                        }
                    }}
        aggregation = {
                        "top_kos": {
                            "terms": {
                                "field": "ko_number",
                                "order": {
                                    "top_hit": "desc"
                                },
                                "size": num
                            },
                        "aggs": {
                            "top_ko": {
                                "top_hits": {'_source': {'includes': ['ko_number', 'ko_name']}, 'size': 1}
                            },
                                "top_hit" : {
                                "max": {
                                    "script": {
                                        "source": "_score"
                                    }
                                }
                            }
                        }
                        }
                    }
        result[index] = []
        sqs_body = self.build_simple_query_string_body(q, **kwargs)
        must = sqs_body['query']
        body = self.build_bool_query_body(must=must, must_not=must_not)
        body['aggs'] = aggregation
        body['size'] = 0
        from_ = kwargs.get('from_', 0)
        r = self.build_es().search(index=index, body=body, size=num, from_=from_)
        return r['aggregations']
        # hits = r['hits']['hits']
        # if hits == []:
        #     result[index] = []
        #     return result
        # else:
        #     for hit in hits:
        #         hit['_source']['_score'] = hit['_score']
        #         result[index].append(hit['_source'])
        #     return result