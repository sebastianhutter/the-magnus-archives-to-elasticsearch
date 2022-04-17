from elasticsearch import Elasticsearch
import logging
import json

class ElasticManagement(object):
    """
        setup elasticsearch connections, create indices and feed data
    """

    def __init__(self, host: str='http://localhost:9200'):
        """
        setup connection to elasticsearch

        :param host: http(s) url for elasticsearch host
        """

        self.host = host
        self.client = Elasticsearch(
            hosts=[self.host]
        )

        if not self.client.ping():
            logging.warning(f'Unable to ping elasticsearch host {self.host}')

    def create_index(self, index_name: str, mappings: dict, settings: dict):
        """
        create the es index with the given name and mapping definition
        :param index_name: name of the index
        :param mappings: mapping definition for index
        :return:
        """

        logging.debug(f'Create index {index_name} with index mappings {json.dumps(mappings, indent=2)}')
        self.client.indices.create(
            index=index_name,
            mappings=mappings,
            settings=settings,
            # ignore if the index already exists
            ignore=400
        )

    def delete_index(self, index_name: str):
        """
        delete the given index
        :param index_name: name of the index to delete
        :return:
        """

        logging.debug(f'Delete index {index_name}')
        self.client.indices.delete(
            index=index_name,
            # ignore index not found
            ignore=404
        )

    def feed_index(self, index_name: str, data: dict, id: str=None):
        """
        feeed the given index with the given data
        :param index_name: name of the index
        :param data: dictionary containing the data
        :param id: optional the id for the document
        :return:
        """

        logging.debug(f'Feed index {index_name} with data {json.dumps(data, indent=2)}')
        self.client.index(index=index_name, body=json.dumps(data), id=id)