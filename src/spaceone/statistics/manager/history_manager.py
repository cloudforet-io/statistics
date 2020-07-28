import logging
from datetime import datetime

from spaceone.core.manager import BaseManager
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
