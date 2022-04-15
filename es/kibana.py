import logging
import requests

class KibanaManagement(object):

    def __init__(self, host: str='http://localhost:5601'):
        self.host = host
        self.headers = {
            'kbn-xsrf': 'reporting'
        }


    def create_index_pattern(self, title: str):

        try:
            r = requests.post(
                url=f'{self.host}/api/index_patterns/index_pattern',
                headers=self.headers,
                json=dict(
                    override=False,
                    refresh_fields=True,
                    index_pattern=dict(
                        title=title
                    )
                ),
            )
            r.raise_for_status()

        except requests.exceptions.RequestException as e:
            if 'Duplicate index pattern' not in r.text:
                raise e

