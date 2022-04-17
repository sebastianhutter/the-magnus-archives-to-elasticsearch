import os.path
from docx import Document
import logging
import re


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
            season=dict(
                type='byte'
            ),
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
            position=dict(
                type='short',
            ),
            type=dict(
                type='keyword',
            ),
            characters=dict(
                type='keyword',
            ),
            line=dict(
                type='text',
                fields=dict(
                    length=dict(
                        type="token_count",
                        analyzer="english"
                    )
                )
            ),
        ),
    )
    # exported via kibana gui -> stack management -> saved objects
    kibana_dashboard = r'''
    {"attributes":{"description":"","hits":0,"kibanaSavedObjectMeta":{"searchSourceJSON":"{\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filter\":[]}"},"optionsJSON":"{\"useMargins\":true,\"syncColors\":false,\"hidePanelTitles\":false}","panelsJSON":"[{\"version\":\"8.1.2\",\"type\":\"lens\",\"gridData\":{\"x\":0,\"y\":0,\"w\":24,\"h\":15,\"i\":\"59e9a378-8916-4b13-86de-71baeadbf947\"},\"panelIndex\":\"59e9a378-8916-4b13-86de-71baeadbf947\",\"embeddableConfig\":{\"attributes\":{\"title\":\"\",\"visualizationType\":\"lnsDatatable\",\"type\":\"lens\",\"references\":[{\"type\":\"index-pattern\",\"id\":\"the_magnus_archives_transcripts\",\"name\":\"indexpattern-datasource-current-indexpattern\"},{\"type\":\"index-pattern\",\"id\":\"the_magnus_archives_transcripts\",\"name\":\"indexpattern-datasource-layer-9f69b758-d8ee-46d1-a7c9-cc4ea0331ae5\"}],\"state\":{\"visualization\":{\"columns\":[{\"columnId\":\"b6d93e0c-794e-448b-bb44-6f90d0e02932\",\"isTransposed\":false},{\"columnId\":\"4fe72c8c-a6f0-481b-b56d-1650ac66f8aa\",\"isTransposed\":false}],\"layerId\":\"9f69b758-d8ee-46d1-a7c9-cc4ea0331ae5\",\"layerType\":\"data\"},\"query\":{\"query\":\"\",\"language\":\"kuery\"},\"filters\":[],\"datasourceStates\":{\"indexpattern\":{\"layers\":{\"9f69b758-d8ee-46d1-a7c9-cc4ea0331ae5\":{\"columns\":{\"b6d93e0c-794e-448b-bb44-6f90d0e02932\":{\"label\":\"Transcripts Parsed\",\"dataType\":\"number\",\"operationType\":\"unique_count\",\"scale\":\"ratio\",\"sourceField\":\"episode_number\",\"isBucketed\":false,\"customLabel\":true},\"4fe72c8c-a6f0-481b-b56d-1650ac66f8aa\":{\"label\":\"Season\",\"dataType\":\"number\",\"operationType\":\"range\",\"sourceField\":\"season\",\"isBucketed\":true,\"scale\":\"interval\",\"params\":{\"type\":\"histogram\",\"ranges\":[{\"from\":0,\"to\":1000,\"label\":\"\"}],\"maxBars\":\"auto\"},\"customLabel\":true}},\"columnOrder\":[\"4fe72c8c-a6f0-481b-b56d-1650ac66f8aa\",\"b6d93e0c-794e-448b-bb44-6f90d0e02932\"],\"incompleteColumns\":{}}}}}}},\"enhancements\":{}}}]","timeRestore":false,"title":"The Magnus Archives Dashboard","version":1},"coreMigrationVersion":"8.1.2","id":"32c54d50-be13-11ec-81b2-97a6366f6ba6","migrationVersion":{"dashboard":"8.1.0"},"references":[{"id":"the_magnus_archives_transcripts","name":"59e9a378-8916-4b13-86de-71baeadbf947:indexpattern-datasource-current-indexpattern","type":"index-pattern"},{"id":"the_magnus_archives_transcripts","name":"59e9a378-8916-4b13-86de-71baeadbf947:indexpattern-datasource-layer-9f69b758-d8ee-46d1-a7c9-cc4ea0331ae5","type":"index-pattern"}],"type":"dashboard","updated_at":"2022-04-17T05:57:00.709Z","version":"WzQ3MTYsOF0="}
    {"excludedObjects":[],"excludedObjectsCount":0,"exportedCount":1,"missingRefCount":0,"missingReferences":[]}
    '''

class MagnusTranscriptLine(object):
    """
        a line in the transcript
    """
    def __init__(self, position: int, line: str, ltype: str, characters: list=None):
        """

        :param position: the position of the line in the transcript
        :param line: the line
        :param ltype: the type - sfx, acting, speaking
        :param actor: the actor speaking the line or doing the instruction
        """

        self.position = position

        self.line = line
        self.line = self.line.strip()
        self.line = self.line.replace('[','')
        self.line = self.line.replace(']', '')
        self.line = self.line.replace('(', '')
        self.line = self.line.replace(')', '')

        self.type = ltype

        self.characters = characters


class MagnusEpisode(object):
    """
        represent a transcript
    """

    def __init__(self, doc: str):

        # placeholder values to fill in during parsing
        self.content_warnings = list()
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

    def _get_season_from_episode(self):
        """
        depending on the epsiode number we return a different season.
        episode 1-40: season 1
        episode 41-80: season 2
        episode 81-120: season 3
        episode 121-160: season 4
        episode 161-200: season 5

        :return: season number
        """

        n = int(self.episode_number)

        if n < 41:
            return 1
        if n < 81:
            return 2
        if n < 121:
            return 3
        if n < 161:
            return 4
        if n < 201:
            return 5

        raise ValueError(f'Unable to get season from episode number {self.episode_number}')

    def _is_content_warning(self, line: str):
        """
        return true if the line marks the beginning of the content warnings

        :param line:
        :return: true or false
        """

        if line.lower().startswith('content warning'):
            return True

        return False

    def _get_content_warning_from_line(self, line: str):
        """
        return a content warning from the given line
        :param line:
        :return:
        """

        return line.replace('- ', '')

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

    def _is_creative_commons_license_line(self, line: str):
        """
        return true if the line is the creative commons license line

        :param line:
        :return: true or false
        """

        if line.lower().startswith('the magnus archives is a podcast distributed by rusty quill and licensed under a creative commons attribution '
                                   'non-commercial sharealike 4.0 international licence'):
            return True

        return False

    def _is_legacy_transcript(self, title: str):
        """
        return true if the given title belongs to a legacy transcript
        :param title: the episode title
        :return: true or false
        """

        pat = re.compile(r'^Case\s\d+[-\w]?')

        if pat.match(title):
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

    def _clear_up_actor_line(self, line: str):
        """
        remove continued snippets and similar from the potential actor line

        :param line:
        :return: cleared up line
        """

        actors_line = line.replace('(CONTINUED)', '')
        actors_line = actors_line.replace('(CONT’D)', '')
        actors_line = actors_line.replace('(CONT\'D)', '')
        actors_line = actors_line.replace('(CON’T)', '')
        actors_line = actors_line.replace('(STATEMENT)', '')
        actors_line = actors_line.replace('(BACKGROUND)', '')
        actors_line = actors_line.replace('(DISTANT)', '')
        actors_line = actors_line.replace('(Cont.)', '')
        actors_line = actors_line.replace('Cont.', '')
        actors_line = actors_line.strip()

        return actors_line

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
            and self._clear_up_actor_line(line).isupper() \
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
        actors_line = self._clear_up_actor_line(line)

        # split up the line by , and AND
        actors = list()
        for actors_line_separated_by_comma in actors_line.split(','):
            for actors_line_separated_by_slash in actors_line_separated_by_comma.strip().split('/'):
                for actors_line_separated_by_AND in actors_line_separated_by_slash.strip().split(' AND '):
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
        pat = re.compile(r'^MAG\s?[-–—]?\s?(?P<number>\d+\.?\d*)\s[-–—]?\s?(?P<title>[\d\w\s’\'\-–"“”]+)$')
        match = pat.match(paragraphs[0].text)

        if not match:
            raise ValueError(f'Unable to parse episode number and title for transcript paragraph "{paragraphs[0].text}"')
        self.episode_title = match.group('title')
        self.episode_number = match.group('number')

        # depending on the epside number we set the season field
        self.season = self._get_season_from_episode()

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

                # in some transcripts the theme outro line is missing. but what should always be there is the license
                # attribution. we don't really require the information for some data experiments. so we make sure
                # to drop out here
                if self._is_creative_commons_license_line(txt):
                    logging.debug(f'creative commons license paragraph')
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

                # and another workaround for the ruleset.
                # the current legacy transcripts for season 02 available for download
                # don't contain any content warnings nor an intro paragraph.
                # the season 02 transcripts contain the word "case \d\d\d\d" in the episode
                # title. if we find this one and we have a sfx line ([CLICK]) then we assume we are inside
                # the episode transcript
                if self._is_legacy_transcript(self.episode_title) \
                        and self._is_sfx_line(txt):
                    is_content_warning = False
                    is_episode_transcript = True

                # with the rules defined we can start to fill in the transcript object
                # if we are parsing content warnings fill them into the matching list
                if is_content_warning:
                    # the replace removes potential leading list signs, like in season 01 e34
                    self.content_warnings.append(self._get_content_warning_from_line(txt))
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
                            position=line_position,
                            line=txt,
                            ltype='sfx'
                        ))
                        last_line_was = 'sfx'
                        continue

                    # if the line starts and ends with ( and ) its an acting instruction
                    if self._is_acting_line(txt):
                        self.lines.append(MagnusTranscriptLine(
                            position=line_position,
                            line=txt,
                            ltype='acting',
                            characters=current_actors
                        ))
                        last_line_was = 'acting'
                        continue

                    # and everything that isn't filtered out yet should be
                    # the spoken lines by the actors
                    self.lines.append(MagnusTranscriptLine(
                        position=line_position,
                        line=txt,
                        ltype='speaking',
                        characters=current_actors
                    ))
                    last_line_was = 'speaking'


    def get_transcript_lines_for_index(self):
        """
        return all transcript lines ready for the index
        :param i: transcript line array position
        :return:
        """

        lines_for_index = list()
        for line in self.lines:
            # get the object data as dictionary and extend it with
            # general information about the episode
            l = line.__dict__
            l['season'] = self.season
            l['episode_number'] = self.episode_number
            l['episode_title'] = self.episode_title
            l['filename'] = self.filename
            l['content_warnings'] = self.content_warnings

            lines_for_index.append(dict(
                document_id=f'{self.episode_number}-{line.position}',
                document=l
            ))

        return lines_for_index