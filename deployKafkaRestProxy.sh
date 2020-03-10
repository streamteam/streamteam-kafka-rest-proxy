#!/bin/bash

#
# StreamTeam
# Copyright (C) 2019  University of Basel
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

die() {
	echo >&2 "$@"
	exit 1
}

IP=10.34.58.65
FOLDER=kafkaRestProxy

#http://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

mvn clean
mvn package

ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "rm -Rf $FOLDER"
ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "mkdir $FOLDER"
ssh -i ~/.ssh/lukasPMAAS ubuntu@$IP "mkdir $FOLDER/target"
scp -i ~/.ssh/lukasPMAAS ./target/streamteam-kafka-rest-proxy-1.0.1-jar-with-dependencies.jar ubuntu@$IP:$FOLDER/target
scp -i ~/.ssh/lukasPMAAS ./startKafkaRestProxy.sh ubuntu@$IP:$FOLDER
scp -i ~/.ssh/lukasPMAAS ./stopKafkaRestProxy.sh ubuntu@$IP:$FOLDER
