#!/usr/bin/env bash
java -cp tests/target/onyx-tests-0.1-SNAPSHOT-shaded.jar:`yarn classpath` edu.snu.onyx.client.JobLauncher "$@"
