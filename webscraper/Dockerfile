FROM python:3.8-slim-buster


WORKDIR /src
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN apt-get update \
    && apt-get -y install libpq-dev gcc
RUN pip install -r requirements.txt
RUN pip install webdriver_manager
RUN apt-get update && apt-get install -y gnupg

RUN pip install pymongo
RUN apt-get install -y wget
RUN apt-get install -y tzdata

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# set display port to avoid crash
ENV DISPLAY=:99

# install selenium
RUN pip install selenium==3.8.0


ADD . /funding
WORKDIR /funding
COPY . .
