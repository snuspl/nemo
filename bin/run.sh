#!/usr/bin/env bash
java -cp `yarn classpath`:target/vortex-0.1-SNAPSHOT-shaded.jar: edu.snu.vortex.client.JobLauncher "$@"
