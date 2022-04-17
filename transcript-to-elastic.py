#!/usr/bin/env python3
import click
import sys
import logging
import os
import glob

from transcript import MagnusEpisode, MagnusEpisodeIndex, MagnusTranscriptIndex
from es import ElasticManagement, KibanaManagement

def initialize_elasticsearch_for_magnus_archives(recreate_indices: bool):
    """
    setup elasticsearch indices
    :return:
    """
    em = ElasticManagement()

    if recreate_indices:
        # delete indexes
        em.delete_index(index_name=MagnusEpisodeIndex.index_name)
        em.delete_index(index_name=MagnusTranscriptIndex.index_name)

    # create indices
    em.create_index(index_name=MagnusEpisodeIndex.index_name, mappings=MagnusEpisodeIndex.index_mappings, settings=MagnusEpisodeIndex.index_settings)
    em.create_index(index_name=MagnusTranscriptIndex.index_name, mappings=MagnusTranscriptIndex.index_mappings, settings=MagnusTranscriptIndex.index_settings)

def initialize_kibana_for_magnus_archives(recreate_kibana_views: bool):
    """
    setup kibana data views
    :return:
    """
    km = KibanaManagement()

    if recreate_kibana_views:
        km.delete_index_pattern(title=MagnusTranscriptIndex.index_name)

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

    # if a directory get all docx files in all subfolders
    files = list()
    for f in glob.glob(os.path.join(path, '**', '*.docx'), recursive=True):
        files.append(f)

    return files


def parse_file_for_magnus_archives(path: str):
    """
    parse the given file
    :param path: path to docx
    :return: MagnusEpside
    """

    return MagnusEpisode(doc=path)

def index_episode_for_magnus_archives(episode: MagnusEpisode):
    """
    send episode to elasitcsearch
    :param episode:
    :return:
    """

    em = ElasticManagement()

    # create or update entry in episode index
    em.feed_index(index_name=MagnusEpisodeIndex.index_name, data=episode.index_document(), id=episode.index_document_id())

    # add all transcript lines to the transcript index
    for l in episode.lines:
        em.feed_index(index_name=MagnusTranscriptIndex.index_name, data=l.index_document(), id=l.index_document_id())

@click.command()
@click.argument(
    'path',
    type=click.Path(exists=True),
    nargs=-1
)
@click.option(
    '--loglevel',
    required=False,
    envvar='LOGLEVEL',
    type=click.Choice(['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']),
    default="INFO",
    help="The loglevel for the script execution",
    show_default=True
)
@click.option(
    '--recreate-indices',
    required=False,
    envvar='RECREATE_INDICES',
    is_flag=True,
    default=False,
    help="Delete and create elasticsearch indices before importing transcripts?",
    show_default=True
)
@click.option(
    '--recreate-kibana-views',
    required=False,
    envvar='RECREATE_KIBANA_VIEWS',
    is_flag=True,
    default=False,
    help="Delete and create kibana views?",
    show_default=True
)
@click.option(
    '--show',
    required=True,
    envvar='SHOW',
    default='magnus',
    type=click.Choice(['magnus']),
    help='Transcripts are parsed for which show?',
    show_default=True
)
def run(path, loglevel, recreate_indices, recreate_kibana_views, show):
    """
    setup elasticsearch and run indexing for a single document or folder

    :return:
    """

    # setup logging
    logging.basicConfig(level=loglevel)
    # we dont want to spam the log if set to debug or info!
    logging.getLogger('elastic_transport.transport').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

    # get all files to parse,
    # if the given filename is a folder, loop over all files in the folder,
    # if its just a single file, get back the single file
    files_to_parse = list()
    for p in path:
        files_to_parse.extend(get_files_to_parse(p))

    # depending on the show we may use different setup and parsing functions
    # at the moment the script only supports magnus archive. but better be prepared!
    if show == 'magnus':
        initialize_elasticsearch_for_magnus_archives(recreate_indices=recreate_indices)
        initialize_kibana_for_magnus_archives(recreate_kibana_views=recreate_kibana_views)

        for f in files_to_parse:
            try:
                parsed_file = parse_file_for_magnus_archives(f)
                index_episode_for_magnus_archives(parsed_file)
            except BaseException as e:
                logging.warning(f'Unable to parse or index document {f}: {e}')

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        logging.error(e)
        sys.exit(1)
