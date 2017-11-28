#!/usr/bin/env bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd $parent_path
java -cp tests/target/onyx-tests-0.1-SNAPSHOT.jar:$1:`yarn classpath` edu.snu.onyx.client.JobLauncher "${@:2}"
