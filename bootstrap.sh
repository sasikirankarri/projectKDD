#!/bin/sh

apt-get install -y openjdk-8-jdk-headless -qq > /dev/null
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH="$JAVA_PATH"/bin:$PATH

pip install -r requirements
