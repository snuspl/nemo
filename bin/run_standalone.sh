#!/usr/bin/env bash
java -cp "target/vortex-0.1-SNAPSHOT-shaded.jar:`yarn classpath`:target/bd17f-1.0-SNAPSHOT.jar" edu.snu.vortex.client.JobLauncher "$@"
