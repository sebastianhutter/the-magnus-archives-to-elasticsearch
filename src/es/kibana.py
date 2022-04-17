import logging
import requests
import time

class KibanaManagement(object):

    def __init__(self, host: str='http://localhost:5601'):
        self.host = host
        self.headers = {
            'kbn-xsrf': 'reporting'
        }

        fail_counter = 0
        while not self._ping():
            if fail_counter >= 10:
                raise ConnectionError(f'Unable to connect to kibana {self.host} after {fail_counter} retries.')
            logging.warning(f'Unable to ping kibana host {self.host}')
            fail_counter += 1
            time.sleep(5)

    def _ping(self):
        """
        simple "ping" for kibana to make waiting for working kibana easy
        :return:
        """

        try:
            r = requests.get(f'{self.host}/api/data_views/default')
            r.raise_for_status()
        except BaseException:
            return False

        return True

    def create_index_pattern(self, title: str):
        """
        create the given index pattern
        :param title:
        :return:
        """

        try:
            r = requests.post(
                url=f'{self.host}/api/index_patterns/index_pattern',
                headers=self.headers,
                json=dict(
                    override=False,
                    refresh_fields=True,
                    index_pattern=dict(
                        title=title,
                        id=title
                    )
                ),
            )
            r.raise_for_status()

        except requests.exceptions.RequestException as e:
            if 'Duplicate index pattern' not in r.text:
                raise e

    def delete_index_pattern(self, title: str):
        """
        delete the given index pattern
        :param title:
        :return:
        """

        try:
            r = requests.delete(
                url=f'{self.host}/api/index_patterns/index_pattern/{title}',
                headers=self.headers,
            )
            r.raise_for_status()

        except requests.exceptions.RequestException as e:
            if not e.response.status_code == 404:
                raise e