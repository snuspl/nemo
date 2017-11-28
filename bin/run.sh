#!/usr/bin/env bash
java -cp target/onyx-examples-0.1-SNAPSHOT.jar:`yarn classpath` edu.snu.onyx.client.JobLauncher "$@"
