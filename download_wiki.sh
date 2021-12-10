#!/bin/sh
set -e

dumplist=$1

while read dump
do
	url=https://dumps.wikimedia.org/enwiki/latest/$dump
	
	wget -c $url
	tar -xvf $dump

done <$dumplist










