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
        if 'key' in options and len(base_df) > 0:
            ascending = not options.get('desc', False)
            try:
                return base_df.sort_values(by=options['key'], ascending=ascending)
            except Exception as e:
                raise ERROR_STATISTICS_QUERY(reason=f'Sorting failed. (sort = {options})')
        else:
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
        empty_join_data = {}
        for stage in query.get('aggregate', []):
            if 'group' in stage:
                group = stage['group']
                for key in group.get('keys', []):
                    if 'name' in key:
                        empty_join_data[key['name']] = []

                for field in group.get('fields', []):
                    if 'name' in field:
                        empty_join_data[field['name']] = []

        return pd.DataFrame(empty_join_data)

    def _join(self, options, domain_id, base_df):
        if 'type' in options and options['type'] not in _JOIN_TYPE_MAP:
            raise ERROR_INVALID_PARAMETER_TYPE(key='aggregate.join.type', type=list(_JOIN_TYPE_MAP.keys()))

        join_keys = options.get('keys')
        join_type = options.get('type', 'LEFT')
        join_df = self._query(options, domain_id, operator='join')

        if len(join_df) == 0:
            join_df = self._generate_empty_data(options['query'])

        try:
            if len(base_df) > 0:
                if join_keys:
                    base_df = pd.merge(base_df, join_df, on=join_keys, how=_JOIN_TYPE_MAP[join_type])
                else:
                    base_df = pd.merge(base_df, join_df, left_index=True, right_index=True,
                                       how=_JOIN_TYPE_MAP[join_type])
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

    # def execute_additional_stat(self, base_results, base_resource_type, base_query, base_extend_data,
    #                             join, concat, fill_na, formulas, page, domain_id):
    #     if len(base_results) == 0:
    #         base_df = self._generate_empty_data(base_query)
    #     else:
    #         base_df = pd.DataFrame(base_results)
    #
    #     base_df = self._extend_data(base_df, base_extend_data)
    #     base_df = self._join(base_df, base_resource_type, join, domain_id)
    #     base_df = self._concat(base_df, concat, domain_id)
    #     if len(fill_na.keys()) > 0:
    #         base_df = base_df.fillna(fill_na)
    #     base_df = self._execute_formula(base_df, formulas)
    #     base_df = self._sort(base_df, sort)
    #     base_df = base_df.replace({np.nan: None})
    #
    #     response = {}
    #     joined_results = base_df.to_dict('records')
    #
    #     if 'limit' in page and page['limit'] > 0:
    #         start = page.get('start', 1)
    #         if start < 1:
    #             start = 1
    #
    #         response['total_count'] = len(joined_results)
    #         response['results'] = joined_results[start - 1:start + page['limit'] - 1]
    #     elif limit:
    #         response['results'] = joined_results[:limit]
    #     else:
    #         response['results'] = joined_results
    #
    #     return response
    #
    # def _execute_formula(self, base_df, formulas):
    #     if len(base_df) > 0:
    #         for formula in formulas:
    #             self._check_formula(formula)
    #             operator = formula.get('operator', 'EVAL')
    #             if operator == 'EVAL':
    #                 base_df = self._execute_formula_eval(base_df, formula['formula'])
    #             elif operator == 'QUERY':
    #                 base_df = self._execute_formula_query(base_df, formula['formula'])
    #
    #     return base_df
    #
    # def _join(self, base_df, base_resource_type, join, domain_id):
    #     for join_query in join:
    #         self._check_join_query(join_query)
    #         response = self.stat(join_query['resource_type'], join_query['query'], domain_id)
    #         join_results = response.get('results', [])
    #         if len(join_results) > 0:
    #             join_df = pd.DataFrame(join_results)
    #         else:
    #             join_df = self._generate_empty_data(join_query['query'])
    #
    #         join_df = self._extend_data(join_df, join_query.get('extend_data', {}))
    #         join_keys = join_query.get('keys')
    #         join_type = join_query.get('type', 'LEFT')
    #         join_resource_type = join_query['resource_type']
    #
    #         try:
    #             if len(base_df) > 0:
    #                 if join_keys:
    #                     base_df = pd.merge(base_df, join_df, on=join_keys, how=_JOIN_TYPE_MAP[join_type])
    #                 else:
    #                     base_df = pd.merge(base_df, join_df, left_index=True, right_index=True,
    #                                        how=_JOIN_TYPE_MAP[join_type])
    #
    #         except Exception as e:
    #             base_columns = base_df.columns.values.tolist()
    #
    #             if join_keys is None:
    #                 raise ERROR_STATISTICS_INDEX_JOIN(reason=str(e))
    #             elif not all(key in base_columns for key in join_keys):
    #                 raise ERROR_STATISTICS_JOIN(resource_type=base_resource_type, join_keys=join_keys)
    #             else:
    #                 raise ERROR_STATISTICS_JOIN(resource_type=join_resource_type, join_keys=join_keys)
    #
    #     return base_df
    #
    # @staticmethod
    # def _generate_empty_data(query):
    #     empty_join_data = {}
    #     group = query.get('aggregate', {}).get('group', {})
    #
    #     for key in group.get('keys', []):
    #         if 'name' in key:
    #             empty_join_data[key['name']] = []
    #
    #     for field in group.get('fields', []):
    #         if 'name' in field:
    #             empty_join_data[field['name']] = []
    #
    #     return pd.DataFrame(empty_join_data)
