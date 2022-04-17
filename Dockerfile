FROM python:3.9

# install requirements
ADD src/requirements.txt /tmp
RUN pip install --upgrade pip \
  && pip install -r /tmp/requirements.txt \
  && rm -f /tmp/requirements.txt

# add script
ADD src/ /srv
RUN rm -f /srv/requirements.txt
WORKDIR /srv
CMD ["./transcript-to-elastic.py", "/transcripts/"]
