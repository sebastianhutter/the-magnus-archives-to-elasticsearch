from docx import Document
import logging
import re

class MagnusEpisodeIndex(object):
    """
        define the elasticsearch index for magnus archive episodes
        the episode index contains metadata of the episode
        a second index will contain the transcript itself
    """

    index_name = 'the_magnus_archives_episodes'
    index_settings = {
        "index.number_of_replicas": 0,
        "index.number_of_shards": 1
    }
    index_mappings = dict(
        properties=dict(
            episode_number=dict(
                type='short',
            ),
            episode_title=dict(
                type='text',
            ),
            content_warnings=dict(
                type='keyword',
            ),
            # can we get the actors from the lines, so we dont duplicate info in the index?
            actors=dict(
                type='keyword',
            ),
        ),
    )

class MagnusTranscriptIndex(object):
    """
        define the elasticsearch index for magnus transcripts
    """

    index_name = 'the_magnus_archives_transcripts'
    index_settings = {
        "index.number_of_replicas": 0,
        "index.number_of_shards": 1
    }
    index_mappings = dict(
        properties=dict(
            episode_number=dict(
                type='short',
            ),
            position=dict(
                type='short',
            ),
            type=dict(
                type='keyword',
            ),
            actor=dict(
                type='keyword',
            ),
            line=dict(
                type='text',
            ),
        ),
    )

class MagnusTranscriptLine(object):
    """
        a line in the transcript
    """
    def __init__(self, episode_number: int, position: int, line: str, ltype: str, actor: str=None):
        """

        :param position: the position of the line in the transcript
        :param line: the line
        :param ltype: the type - sfx, acting, speaking
        :param actor: the actor speaking the line or doing the instruction
        """

        self.episode_number = episode_number
        self.position = position

        self.line = line
        self.line = self.line.strip()
        self.line = self.line.replace('[','')
        self.line = self.line.replace(']', '')
        self.line = self.line.replace('(', '')
        self.line = self.line.replace(')', '')

        self.type = ltype

        self.actor = actor

    def index_document_id(self):
        """
        return the id for the elasticsearch document
        :return:
        """
        return f'{self.episode_number}-{self.position}'

    # thats not very neat, as it relays on setting the correct fields here and in the index object
    # something like marhsmallow would be neater ... but good enough for now
    def index_document(self):
        """
        returns the content of the object in a format usable by es
        :return: dict with index object fields
        """

        return self.__dict__


class MagnusEpisode(object):
    """
        represent a transcript
    """

    def __init__(self, doc: str):

        # placeholder values to fill in during parsing
        self.content_warnings = list()
        self.actors = list()
        self.lines = list()

        self._load(doc)
        self._parse()

    def _load(self, doc: str):
        """
        load transcript from doc

        :param doc: path to word document containing the transcript
        :return:
        """

        logging.info(f'Load transcript doc {doc}')
        with open(doc, 'rb') as f:
            self.document = Document(f)


    def _parse(self):
        """
        parse the given transcript document.
        parsing is done by iterating over the given paragraphs
        and hopefully all documents follow the same structure ;-)

        :return:
        """

        ignore_paragraphs = [
            None,
            '',
            '[The Magnus Archives Theme – Intro - Continues]',
            '[Main Body of Statement]'
        ]

        paragraphs = self.document.paragraphs

        # the first paragraph is contains the title and episode number
        # some paragraphs look like this "MAG 187 — Checking Out" some like "MAG – 012 – First Aid"
        # so we need to do some regexp magic
        pat = re.compile(r'^MAG\s[-–—]?\s?(?P<number>\d+)\s[-–—]?\s?(?P<title>[\w\s’\'"]+)$')
        match = pat.match(paragraphs[0].text)

        if not match:
            raise ValueError(f'Unable to parse episode number and title for transcript paragraph "{paragraphs[0].text}"')
        self.episode_title = match.group('title')
        self.episode_number = match.group('number')

        # now we loop trough all the paragraphs and load the different values
        # depending on some content rules
        is_content_warning = False
        is_episode_transcript = False
        current_actor = None

        i = 1
        for p in paragraphs[i:]:
            txt = p.text
            logging.debug(f'{i}: {txt}')

            # ignore paragraphs
            if txt in ignore_paragraphs:
                logging.debug(f'ignoring paragraph: {txt}')
                continue

            # if we stumble over the content warnings label all
            # upcoming values are content warnings.
            # this is true until the first [] line - usually the theme song
            if txt.lower() == 'content warnings':
                logging.debug('content warning paragraph')
                is_content_warning = True
                continue

            # we are inside the episode transcript after the theme music has played
            # slight differences in chars -, – and – ;-)
            if txt.lower() == '[the magnus archives theme - intro]' \
                    or txt.lower() == '[the magnus archives theme – intro]' \
                    or txt.lower() == '[the magnus archives theme – intro]':
                logging.debug('theme intro paragraph')
                is_content_warning = False
                is_episode_transcript = True
                continue
            # and after the outro music we arent inside the content anymore
            if txt.lower() == '[the magnus archives theme - outro]' \
                    or txt.lower() == '[the magnus archives theme – outro]' \
                or txt.lower() == '[the magnus archives theme – outro]':
                logging.debug('theme outro paragraph')
                is_episode_transcript = False
                continue


            # with the rules defined we can start to fill in the transcript object
            # if we are parsing content warnings fill them into the matching list
            if is_content_warning:
                self.content_warnings.append(txt)
                continue

            # if we are "inside" the episode
            if is_episode_transcript:
                # if the text is all UPPERCASE we know that that the current line is the ACTOR
                # and we know that all following lines are spoken by the actor.
                # except for sfx and acting guidance
                if txt.isupper():
                    # strip CONTINUED from the text before checking
                    a = txt.replace('(CONTINUED)','')
                    a = a.replace('(CONT’D)','')
                    a = a.strip()
                    if a not in self.actors:
                        self.actors.append(a)

                    # set current actor, to ensure we can assign the transcript lines to the actor
                    logging.debug(f'actor paragraph {a}')
                    current_actor = a
                    continue

                # we should be in a place now where we only have "content"
                # so lets figure out which our current position in the transcript is
                # to ensure we have the correct ordering in place
                line_position = len(self.lines) + 1 if len(self.lines) > 0 else 0

                # if the line starts and ends with [ and ] its an sfx instruction
                if txt.startswith('[') and txt.endswith(']'):
                    self.lines.append(MagnusTranscriptLine(
                        episode_number=self.episode_number,
                        position=line_position,
                        line=txt,
                        ltype='sfx'
                    ))
                    continue

                # if the line starts and ends with ( and ) its an acting instruction
                if txt.startswith('(') and txt.endswith(')'):
                    self.lines.append(MagnusTranscriptLine(
                        episode_number=self.episode_number,
                        position=line_position,
                        line=txt,
                        ltype='acting',
                        actor=current_actor
                    ))
                    continue

                # and everything that isnt filtered out yet should be
                # the spoken lines by the actors
                self.lines.append(MagnusTranscriptLine(
                    episode_number=self.episode_number,
                    position=line_position,
                    line=txt,
                    ltype='speaking',
                    actor=current_actor
                ))

            i += 1

    def index_document_id(self):
        """
        return the id for the elasticsearch document
        :return:
        """
        return self.episode_number

    # thats not very neat, as it relays on setting the correct fields here and in the index object
    # something like marhsmallow would be neater ... but good enough for now
    def index_document(self):
        """
        returns the content of the object in a format usable by es
        :return: dict with index object fields
        """

        return dict(
            episode_number=self.episode_number,
            episode_title=self.episode_title,
            content_warnings=self.content_warnings,
            actors=self.actors,
        )

    