import logging
import pandas as pd

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


class ResourceManager(BaseManager):

    def stat(self, resource_type, query, domain_id):
        self.service_connector: ServiceConnector = self.locator.get_connector('ServiceConnector')
        service, resource = self._parse_resource_type(resource_type)

        try:
            return self.service_connector.stat_resource(service, resource, query, domain_id)
        except ERROR_BASE as e:
            raise ERROR_STATISTICS_QUERY(reason=e.message)
        except Exception as e:
            raise ERROR_STATISTICS_QUERY(reason=e)

    def join_and_execute_formula(self, base_results, base_resource_type, base_query, join, formulas,
                                 sort, page, limit, domain_id):
        if len(base_results) == 0:
            base_df = self._generate_empty_join_data(base_query)
        else:
            base_df = pd.DataFrame(base_results)

        base_df = self._join(base_df, base_resource_type, join, domain_id)
        base_df = base_df.fillna(0)
        base_df = self._execute_formula(base_df, formulas)
        base_df = self._sort(base_df, sort)

        response = {}
        joined_results = base_df.to_dict('records')

        if 'limit' in page and page['limit'] > 0:
            start = page.get('start', 1)
            if start < 1:
                start = 1

            response['total_count'] = len(joined_results)
            response['results'] = joined_results[start - 1:start + page['limit'] - 1]
        elif limit:
            response['results'] = joined_results[:limit]
        else:
            response['results'] = joined_results

        return response

    def _execute_formula(self, base_df, formulas):
        if len(base_df) > 0:
            for formula in formulas:
                self._check_formula(formula)
                operator = formula.get('operator', 'EVAL')
                if operator == 'EVAL':
                    base_df = self._execute_formula_eval(base_df, formula['name'], formula['formula'])
                elif operator == 'QUERY':
                    base_df = self._execute_formula_query(base_df, formula['formula'])

        return base_df

    @staticmethod
    def _execute_formula_query(base_df, formula):
        try:
            base_df = base_df.query(formula)
        except Exception as e:
            raise ERROR_STATISTICS_FORMULA(formula=formula)

        return base_df

    @staticmethod
    def _execute_formula_eval(base_df, name, formula):
        try:
            base_df[name] = base_df.eval(formula)
        except Exception as e:
            raise ERROR_STATISTICS_FORMULA(formula=formula)

        return base_df

    def _join(self, base_df, base_resource_type, join, domain_id):
        for join_query in join:
            self._check_join_query(join_query)
            response = self.stat(join_query['resource_type'], join_query['query'], domain_id)
            join_results = response.get('results', [])
            if len(join_results) > 0:
                join_df = pd.DataFrame(join_results)
            else:
                join_df = self._generate_empty_join_data(join_query['query'])

            join_keys = join_query.get('keys')
            join_type = join_query.get('type', 'LEFT')
            join_resource_type = join_query['resource_type']

            try:
                if len(base_df) > 0:
                    if join_keys:
                        base_df = pd.merge(base_df, join_df, on=join_keys, how=_JOIN_TYPE_MAP[join_type])
                    else:
                        base_df = pd.merge(base_df, join_df, left_index=True, right_index=True,
                                           how=_JOIN_TYPE_MAP[join_type])

            except Exception as e:
                base_columns = base_df.columns.values.tolist()

                if join_keys is None:
                    raise ERROR_STATISTICS_INDEX_JOIN(reason=str(e))
                elif not all(key in base_columns for key in join_keys):
                    raise ERROR_STATISTICS_JOIN(resource_type=base_resource_type, join_keys=join_keys)
                else:
                    raise ERROR_STATISTICS_JOIN(resource_type=join_resource_type, join_keys=join_keys)

        return base_df

    @staticmethod
    def _generate_empty_join_data(query):
        empty_join_data = {}
        group = query.get('aggregate', {}).get('group', {})

        for key in group.get('keys', []):
            if 'name' in key:
                empty_join_data[key['name']] = []

        for field in group.get('fields', []):
            if 'name' in field:
                empty_join_data[field['name']] = []

        return pd.DataFrame(empty_join_data)

    @staticmethod
    def _sort(base_df, sort):
        if (sort and 'name' in sort) and len(base_df) > 0:
            ascending = not sort.get('desc', False)
            try:
                return base_df.sort_values(by=sort['name'], ascending=ascending)
            except Exception as e:
                raise ERROR_STATISTICS_QUERY(reason=f'Sorting failed. (sort = {sort})')
        else:
            return base_df

    @staticmethod
    def _parse_resource_type(resource_type):
        try:
            service, resource = resource_type.split('.')
        except Exception as e:
            raise ERROR_INVALID_PARAMETER(key='resource_type', reason=f'resource_type is invalid. ({resource_type})')

        return service, resource

    @staticmethod
    def _check_formula(formula):
        operator = formula.get('operator', 'EVAL')

        if operator not in ['EVAL', 'QUERY']:
            raise ERROR_INVALID_PARAMETER(key='formulas.operator', reason='The operator only allows EVAL or QUERY.')

        if operator == 'EVAL' and 'name' not in formula:
            raise ERROR_REQUIRED_PARAMETER(key='formulas.name')

        if 'formula' not in formula:
            raise ERROR_REQUIRED_PARAMETER(key='formulas.formula')

    @staticmethod
    def _check_join_query(join_query):
        if 'type' in join_query and join_query['type'] not in _JOIN_TYPE_MAP:
            raise ERROR_INVALID_PARAMETER_TYPE(key='join.type', type=list(_JOIN_TYPE_MAP.keys()))

        if 'resource_type' not in join_query:
            raise ERROR_REQUIRED_PARAMETER(key='join.resource_type')

        if 'query' not in join_query:
            raise ERROR_REQUIRED_PARAMETER(key='join.query')
