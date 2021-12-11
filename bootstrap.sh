#!/bin/sh

apt-get install -y openjdk-8-jdk-headless -qq > /dev/null


pip install -r requirements

sh download_wiki.sh dumplist
