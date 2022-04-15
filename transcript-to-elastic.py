#!/usr/bin/env python3
import click
import sys
import logging
import os

from transcript import MagnusEpisode, MagnusEpisodeIndex, MagnusTranscriptIndex
from es import ElasticManagement, KibanaManagement

# setup logging
logging.basicConfig(level=logging.INFO)

def initialize_elasticsearch():
    """
    setup elasticsearch indices
    :return:
    """
    em = ElasticManagement()

    # create indices
    em.create_index(index_name=MagnusEpisodeIndex.index_name, mappings=MagnusEpisodeIndex.index_mappings, settings=MagnusEpisodeIndex.index_settings)
    em.create_index(index_name=MagnusTranscriptIndex.index_name, mappings=MagnusTranscriptIndex.index_mappings, settings=MagnusTranscriptIndex.index_settings)

def initialize_kibana():
    """
    setup kibana data views
    :return:
    """
    km = KibanaManagement()

    km.create_index_pattern(title=MagnusEpisodeIndex.index_name)
    km.create_index_pattern(title=MagnusTranscriptIndex.index_name)

def get_files_to_parse(path: str):
    """
    check if the given path is a file or a folder
    if a folder return all documents in the folder, else just return the file
    :param path: path to file or folder
    :return: list of paths
    """

    # if the given path is a file, return it
    if os.path.isfile(path):
        return [path]

    # if not get all files in the directory
    return [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]


def load_and_index_file(path: str):
    """
    parses the given word document and loads it into elasticsearch
    :param path: path to file
    :return:
    """

    episode = MagnusEpisode(doc=path)
    em = ElasticManagement()

    # create or update entry in episode index
    em.feed_index(index_name=MagnusEpisodeIndex.index_name, data=episode.index_document(), id=episode.index_document_id())

    # add all transcript lines to the transcript index
    for l in episode.lines:
        em.feed_index(index_name=MagnusTranscriptIndex.index_name, data=l.index_document(), id=l.index_document_id())


@click.command()
@click.argument(
    'filename',
    type=click.Path(exists=True),
)
def run(filename):
    """
    setup elasticsearch and run indexing for a single document or folder
    :param filename:
    :return:
    """

    initialize_elasticsearch()
    initialize_kibana()

    # if the given filename is a folder,
    # loop over all files in the folder,
    # if its a file just parse the single file
    files_to_parse = get_files_to_parse(filename)

    for f in files_to_parse:
        try:
            load_and_index_file(f)
        except BaseException as e:
            logging.warning(f'Unable to index document {f}: {e}')


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        logging.error(e)
        sys.exit(1)
