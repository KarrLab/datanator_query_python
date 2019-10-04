from karr_lab_aws_manager.elasticsearch import query_builder as es_query_builder


class FTX(es_query_builder.QueryBuilder):

    def __init__(self, profile_name=None, credential_path=None,
                config_path=None, elastic_path=None,
                cache_dir=None, service_name='es', max_entries=float('inf'), verbose=False):
        super().__init__(profile_name=profile_name, credential_path=credential_path,
                config_path=config_path, elastic_path=elastic_path,
                cache_dir=cache_dir, service_name=service_name, max_entries=max_entries, verbose=verbose)