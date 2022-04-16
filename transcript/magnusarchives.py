import os.path
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
    index_settings = dict(
        index=dict(
            number_of_replicas=0,
            number_of_shards=1,
            sort=dict(
                field=['episode_number'],
                order=['asc']
            )
        )
    )
    index_mappings = dict(
        properties=dict(
            episode_number=dict(
                type='short',
            ),
            episode_title=dict(
                type='text',
            ),
            filename=dict(
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
    index_settings = dict(
        index=dict(
            number_of_replicas=0,
            number_of_shards=1,
            sort=dict(
                field=['episode_number', 'position'],
                order=['asc', 'asc']
            )
        )
    )
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
            actors=dict(
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
    def __init__(self, episode_number: int, position: int, line: str, ltype: str, actors: list=None):
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

        self.actors = actors

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
        self.filename = os.path.basename(doc)

        self.paragraphs_to_ignore_in_transcripts = [
            None,
            '',
            '[The Magnus Archives Theme – Intro - Continues]',
            '[Main Body of Statement]'
        ]

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

    def _is_content_warning(self, line: str):
        """
        return true if the line marks the beginning of the content warnings

        :param line:
        :return: true or false
        """

        if line.lower().startswith('content warning'):
            return True

        return False

    def _is_theme_intro(self, line: str):
        """
        return true if the line is the theme intro line

        :param line:
        :return: true or false
        """

        l = line.lower()

        if l.startswith('['):
            if l.endswith(' intro]') \
                or l.endswith(' -intro]') \
                    or l.endswith(' into]'):
                    return True

            return False

    def _is_theme_outro(self, line: str):
        """
        return true if the line is the theme outro line

        :param line:
        :return: true or false
        """

        l = line.lower()

        if l.startswith('['):
            if l.endswith(' outro]') \
                or l.endswith(' -outro]'):
                return True

        return False

    def _is_sfx_line(self, line: str):
        """
        return true if the line is a sfx instruction
        a sfx instruction starts and ends with [ and ]

        :param line:
        :return: true or false
        """

        if line.startswith('[') and line.endswith(']'):
            return True

        # in some cases we got char errors, damn you keyboard layouts ;-)
        if line.startswith('{') and line.endswith(']'):
            return True
        if line.startswith('[') and line.endswith('}'):
            return True

        return False



    def _is_acting_line(self, line: str):
        """
        return true if the line is an acting instruction
        an acting instruction starts and ends with ( and )

        :param line:
        :return: true or false
        """

        if line.startswith('(') and line.endswith(')'):
            return True

        return False

    def _is_actor_line(self, line: str):
        """
        return true if the line is an actor line
        an actor line is usually a line that only contains word characters all in uppercase

        :param line:
        :return: true or false
        """

        # check if line isnt empty, check if line is all in uppercase,
        # ensure no sfx or acting instructions are given
        if line \
            and line.isupper() \
            and not self._is_sfx_line(line) \
                and not self._is_acting_line(line):

            return True

        return False

    def _get_actors_from_actor_line(self, line: str):
        """
        lets split the actor line up. in some transcripts multiple actors are specified
        i found one occurence with AND, but I also assume we got some , in there ;-)

        :param line: actor line
        :return: list of actors
        """

        # strip CONTINUED from the text before checking
        actors_line = line.replace('(CONTINUED)', '')
        actors_line = actors_line.replace('(CONT’D)', '')
        actors_line = actors_line.replace('(STATEMENT)', '')
        actors_line = actors_line.strip()

        # split up the line by , and AND
        actors = list()
        for actors_line_separated_by_comma in actors_line.split(','):
            for actors_line_separated_by_slash in actors_line_separated_by_comma.strip().split('/'):
                for actors_line_separated_by_AND in actors_line_separated_by_slash.strip().split('AND'):
                    actors.append(actors_line_separated_by_AND.strip())

        return actors


    def _parse(self):
        """
        parse the given transcript document.
        parsing is done by iterating over the given paragraphs
        and hopefully all documents follow the same structure ;-)

        :return:
        """

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
        # store the last type of line when parsing
        # the transcript to allow better rules
        # like if last line was actor line (ALL UPPERCASE)
        # the next line will certainly be a spoken line, independent if its all UPPERCASE again
        last_line_was = None

        for p in paragraphs[1:]:
            # get the paragraph, remove trailing and leading whitespaces
            paragrah_text = p.text
            paragrah_text = paragrah_text.strip()

            # as we are dealing with word documents some paragraphs contain
            # line breaks which hide meta information (e.g. ACTOR\lntext)
            # to ensure we can properly parse all lines we need to break them up again
            for txt in paragrah_text.splitlines():
                # ignore paragraphs
                if txt in self.paragraphs_to_ignore_in_transcripts:
                    logging.debug(f'ignoring paragraph: "{txt}"')
                    continue

                # if we stumble over the content warnings label all
                # upcoming values are content warnings.
                # this is true until the first [] line - usually the theme song
                if self._is_content_warning(txt):
                    logging.debug(f'content warning paragraph')
                    is_content_warning = True
                    continue

                # we are inside the episode transcript after the theme music has played
                # slight differences in chars -, – and – ;-)
                if self._is_theme_intro(txt):
                    logging.debug(f'theme intro paragraph')
                    is_content_warning = False
                    is_episode_transcript = True
                    continue

                # and after the outro music we aren't inside the content anymore
                if self._is_theme_outro(txt):
                    logging.debug(f'theme outro paragraph')
                    is_episode_transcript = False
                    continue

                # here is a hack for our ruleset
                # in some transcripts the theme intro paragraph is missing (season 01, episode 22)
                # in this case we try the following,
                # if we are inside a content_warning loop but the current line is an sfx, an actor instruction
                # or an acting instruction we disable the content_warning loop and enable the transcript loop
                if is_content_warning \
                    and (self._is_sfx_line(txt) or self._is_acting_line(txt) or self._is_actor_line(txt)):
                    is_content_warning = False
                    is_episode_transcript = True

                # with the rules defined we can start to fill in the transcript object
                # if we are parsing content warnings fill them into the matching list
                if is_content_warning:
                    # the replace removes potential leading list signs, like in season 01 e34
                    self.content_warnings.append(txt.replace('- ', ''))
                    continue

                # if we are "inside" the episode
                if is_episode_transcript:
                    logging.debug(f'"{txt}"')

                    # if the text is all UPPERCASE we assume we have an actor line
                    # now there are a few exceptions, of course ;-)
                    # first: in some transcripts sfx and acting instructions are all in UPPERCASE, we need to filter them out too
                    # second: in some transcripts different newlines are used, so we need to split these lines up
                    # third: in some transcripts multiple actors are specified, so we need to split those up
                    # fourth: in some transcripts the spoken line by the actor is also in all UPPERCASE so we need to make sure to ignore these
                    if self._is_actor_line(txt) and last_line_was != 'actor':
                        current_actors = self._get_actors_from_actor_line(txt)
                        for a in current_actors:
                            if a not in self.actors:
                                self.actors.append(a)

                        # set current actor, to ensure we can assign the transcript lines to the actor
                        logging.debug(f'actor paragraph: "{current_actors}"')
                        last_line_was = 'actor'
                        continue

                    # we should be in a place now where we only have "content"
                    # so lets figure out which our current position in the transcript is
                    # to ensure we have the correct ordering in place
                    line_position = len(self.lines) + 1 if len(self.lines) > 0 else 1

                    # if the line starts and ends with [ and ] its an sfx instruction
                    if self._is_sfx_line(txt):
                        logging.debug(f'sfx paragraph: "{txt}"')
                        self.lines.append(MagnusTranscriptLine(
                            episode_number=self.episode_number,
                            position=line_position,
                            line=txt,
                            ltype='sfx'
                        ))
                        last_line_was = 'sfx'
                        continue

                    # if the line starts and ends with ( and ) its an acting instruction
                    if self._is_acting_line(txt):
                        self.lines.append(MagnusTranscriptLine(
                            episode_number=self.episode_number,
                            position=line_position,
                            line=txt,
                            ltype='acting',
                            actors=current_actors
                        ))
                        last_line_was = 'acting'
                        continue

                    # and everything that isn't filtered out yet should be
                    # the spoken lines by the actors
                    self.lines.append(MagnusTranscriptLine(
                        episode_number=self.episode_number,
                        position=line_position,
                        line=txt,
                        ltype='speaking',
                        actors=current_actors
                    ))
                    last_line_was = 'speaking'


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
            filename=self.filename
        )

    