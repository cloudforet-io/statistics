import logging
import pandas as pd
from datetime import datetime

from spaceone.core.manager import BaseManager
from spaceone.statistics.error import *
from spaceone.statistics.model.history_model import History

_LOGGER = logging.getLogger(__name__)


class HistoryManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.history_model: History = self.locator.get_model('History')

    def create_history(self, schedule_vo, topic, results, domain_id):
        def _rollback(history_vo):
            _LOGGER.info(f'[create_history._rollback] '
                         f'Delete history : {history_vo.topic}')
            history_vo.delete()

        created_at = datetime.utcnow()

        for values in results:
            history_data = {
                'topic': topic,
                'schedule': schedule_vo,
                'values': values,
                'created_at': created_at,
                'domain_id': domain_id
            }

            _LOGGER.debug(f'[create_history] create history: {history_data}')

            history_vo: History = self.history_model.create(history_data)

            self.transaction.add_rollback(_rollback, history_vo)

    def list_history(self, query={}):
        return self.history_model.query(**query)

    def stat_history(self, query):
        return self.history_model.stat(**query)

    def diff_history(self, before_data, after_data, default_fields, diff_fields):
        pd.set_option('display.max_columns', None)
        all_fields = default_fields + diff_fields
        before_df = pd.DataFrame(before_data, columns=all_fields)
        after_df = pd.DataFrame(after_data, columns=all_fields)

        _LOGGER.debug(f'[before data frame] >>\n{before_df}')
        _LOGGER.debug(f'[after data frame] >>\n{after_df}')

        default_columns_df, diff_columns_df = self._join_history(before_df, after_df, default_fields, diff_fields)
        diff_df = self._diff_history(default_columns_df, diff_columns_df)

        results = self._make_results(diff_df, default_fields, diff_fields)
        return results

    @staticmethod
    def _make_results(diff_df, default_fields, diff_fields):
        results = []
        for row in diff_df.to_dict('records'):
            values = {}
            is_change = False
            for field in diff_fields:
                diff_value = row[f'{field}_after']
                if diff_value != 0:
                    is_change = True

                values[field] = diff_value

            for field in default_fields:
                values[field] = row[field]

            if is_change:
                results.append(values)

        final_df = pd.DataFrame(results)
        _LOGGER.debug(f'[final data frame] >>\n{final_df}')

        return results

    @staticmethod
    def _diff_history(default_columns_df, diff_columns_df):
        diff_df = diff_columns_df.diff(axis=1, periods=1)
        _LOGGER.debug(f'[diff data frame] >>\n{diff_df}')
        diff_df = default_columns_df.merge(diff_df, right_index=True, left_index=True)
        _LOGGER.debug(f'[diff merged data frame] >>\n{diff_df}')

        return diff_df

    def _join_history(self, before_df, after_df, default_fields, diff_fields):
        try:
            joined_df = before_df.merge(after_df, how='outer', on=default_fields, suffixes=('_before', '_after'))
        except Exception as e:
            raise ERROR_NOT_FOUND_DIFF_FIELDS(field_type='default_fields', fields=default_fields)
        joined_df = joined_df.fillna(0.0)
        _LOGGER.debug(f'[joined data frame] >>\n{joined_df}')
        return joined_df[default_fields], self._get_diff_columns(joined_df, diff_fields)

    @staticmethod
    def _get_diff_columns(joined_df, diff_fields):
        fields = []
        for field in diff_fields:
            fields.append(f'{field}_before')
            fields.append(f'{field}_after')
        return joined_df[fields]
