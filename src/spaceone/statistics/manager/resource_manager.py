import logging
import pandas as pd
import numpy as np

from spaceone.core.manager import BaseManager
from spaceone.statistics.error import *
from spaceone.statistics.connector.service_connector import ServiceConnector

_LOGGER = logging.getLogger(__name__)

_JOIN_TYPE_MAP = {
    'LEFT': 'left',
    'RIGHT': 'right',
    'OUTER': 'outer',
    'INNER': 'inner'
}
_SUPPORTED_AGGREGATE_OPERATIONS = [
    'query',
    'join',
    'concat',
    'sort',
    'formula',
    'fill_na'
]


class ResourceManager(BaseManager):

    def stat(self, aggregate, page, domain_id):
        results = self._execute_aggregate_operations(aggregate, domain_id)
        return self._page(page, results)

    def _execute_aggregate_operations(self, aggregate, domain_id):
        df = None

        if 'query' not in aggregate[0]:
            raise ERROR_REQUIRED_QUERY_OPERATION()

        for stage in aggregate:
            if 'query' in stage:
                df = self._query(stage['query'], domain_id)

            elif 'join' in stage:
                df = self._join(stage['join'], domain_id, df)

            elif 'concat' in stage:
                df = self._concat(stage['concat'], domain_id, df)

            elif 'sort' in stage:
                df = self._sort(stage['sort'], df)

            elif 'formula' in stage:
                df = self._execute_formula(stage['formula'], df)

            elif 'fill_na' in stage:
                df = self._fill_na(stage['fill_na'], df)

            else:
                raise ERROR_REQUIRED_PARAMETER(key='aggregate.query | aggregate.join | aggregate.concat | '
                                                   'aggregate.sort | aggregate.formula | aggregate.fill_na')

        df = df.replace({np.nan: None})
        results = df.to_dict('records')

        return results

    @staticmethod
    def _fill_na(options, base_df):
        data = options.get('data', {})
        if len(data.keys()) > 0:
            base_df = base_df.fillna(data)

        return base_df

    def _execute_formula(self, options, base_df):
        if len(base_df) > 0:
            if 'eval' in options:
                base_df = self._execute_formula_eval(options['eval'], base_df)
            elif 'query' in options:
                base_df = self._execute_formula_query(options['query'], base_df)
            else:
                raise ERROR_REQUIRED_PARAMETER(key='aggregate.formula.eval | aggregate.formula.query')

        return base_df

    @staticmethod
    def _execute_formula_query(formula, base_df):
        try:
            base_df = base_df.query(formula)
        except Exception as e:
            raise ERROR_STATISTICS_FORMULA(formula=formula)

        return base_df

    @staticmethod
    def _execute_formula_eval(formula, base_df):
        try:
            base_df = base_df.eval(formula)
        except Exception as e:
            raise ERROR_STATISTICS_FORMULA(formula=formula)

        return base_df

    @staticmethod
    def _sort(options, base_df):
        if len(base_df) > 0:
            if 'key' in options:
                ascending = not options.get('desc', False)
                try:
                    return base_df.sort_values(by=options['key'], ascending=ascending)
                except Exception as e:
                    raise ERROR_STATISTICS_QUERY(reason=f'Sorting failed. (sort = {options})')
            elif 'keys' in options:
                keys = []
                ascendings = []
                for sort_options in options.get('keys', []):
                    key = sort_options.get('key')
                    ascending = not sort_options.get('desc', False)

                    if key:
                        keys.append(key)
                        ascendings.append(ascending)

                try:
                    return base_df.sort_values(by=keys, ascending=ascendings)
                except Exception as e:
                    raise ERROR_STATISTICS_QUERY(reason=f'Sorting failed. (sort = {options})')

        return base_df

    def _concat(self, options, domain_id, base_df):
        concat_df = self._query(options, domain_id, operator='join')

        try:
            base_df = pd.concat([base_df, concat_df], ignore_index=True)
        except Exception as e:
            raise ERROR_STATISTICS_CONCAT(reason=str(e))

        return base_df

    @staticmethod
    def _generate_empty_data(query):
        empty_data = {}
        aggregate = query.get('aggregate', [])
        aggregate.reverse()
        for stage in aggregate:
            if 'group' in stage:
                group = stage['group']
                for key in group.get('keys', []):
                    if 'name' in key:
                        empty_data[key['name']] = []

                for field in group.get('fields', []):
                    if 'name' in field:
                        empty_data[field['name']] = []

                break

        return pd.DataFrame(empty_data)

    def _join(self, options, domain_id, base_df):
        if 'type' in options and options['type'] not in _JOIN_TYPE_MAP:
            raise ERROR_INVALID_PARAMETER_TYPE(key='aggregate.join.type', type=list(_JOIN_TYPE_MAP.keys()))

        join_keys = options.get('keys')
        join_type = options.get('type', 'LEFT')
        join_df = self._query(options, domain_id, operator='join')

        try:
            if join_keys:
                base_df = pd.merge(base_df, join_df, on=join_keys, how=_JOIN_TYPE_MAP[join_type])
            else:
                base_df = pd.merge(base_df, join_df, left_index=True, right_index=True, how=_JOIN_TYPE_MAP[join_type])
        except Exception as e:
            if join_keys is None:
                raise ERROR_STATISTICS_INDEX_JOIN(reason=str(e))
            else:
                raise ERROR_STATISTICS_JOIN(resource_type=options['resource_type'], join_keys=join_keys)

        return base_df

    def _query(self, options, domain_id, operator='query'):
        resource_type = options.get('resource_type')
        query = options.get('query')
        extend_data = options.get('extend_data', {})

        if resource_type is None:
            raise ERROR_REQUIRED_PARAMETER(key=f'aggregate.{operator}.resource_type')

        if query is None:
            raise ERROR_REQUIRED_PARAMETER(key=f'aggregate.{operator}.query')

        self.service_connector: ServiceConnector = self.locator.get_connector('ServiceConnector')
        service, resource = self._parse_resource_type(resource_type)

        try:
            response = self.service_connector.stat_resource(service, resource, query, domain_id)
            results = response.get('results', [])

            if len(results) > 0 and not isinstance(results[0], dict):
                df = pd.DataFrame(results, columns=['value'])
            else:
                df = pd.DataFrame(results)

                if len(df) == 0:
                    df = self._generate_empty_data(options['query'])

            return self._extend_data(df, extend_data)

        except ERROR_BASE as e:
            raise ERROR_STATISTICS_QUERY(reason=e.message)
        except Exception as e:
            raise ERROR_STATISTICS_QUERY(reason=e)

    @staticmethod
    def _parse_resource_type(resource_type):
        try:
            service, resource = resource_type.split('.')
        except Exception as e:
            raise ERROR_INVALID_PARAMETER(key='resource_type', reason=f'resource_type is invalid. ({resource_type})')

        return service, resource

    @staticmethod
    def _extend_data(df, data):
        for key, value in data.items():
            df[key] = value

        return df

    @staticmethod
    def _page(page, results):
        response = {
            'total_count': len(results)
        }

        if 'limit' in page and page['limit'] > 0:
            start = page.get('start', 1)
            if start < 1:
                start = 1

            response['results'] = results[start - 1:start + page['limit'] - 1]
        else:
            response['results'] = results

        return response
