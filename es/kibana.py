import logging
import requests

class KibanaManagement(object):

    def __init__(self, host: str='http://localhost:5601'):
        self.host = host
        self.headers = {
            'kbn-xsrf': 'reporting'
        }


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