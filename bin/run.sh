#!/usr/bin/env bash
java -cp target/onyx-project-0.1-SNAPSHOT.jar:`yarn classpath` edu.snu.onyx.client.JobLauncher "$@"
