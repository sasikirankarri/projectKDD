#!/bin/sh
set -e

dumplist=$1

while read dump
do
	wget -c $dump 
	

done <$dumplist










