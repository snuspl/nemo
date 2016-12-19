# Vortex 
[![Build Status](http://cmscluster.snu.ac.kr:8080/jenkins/buildStatus/icon?job=Vortex-master)](http://cmscluster.snu.ac.kr:8080/jenkins/job/Vortex-master/)

Vortex is a general-purpose data-processing engine.

## Requirements
* Beam 0.4.0-incubating-SNAPSHOT (You must download it from https://github.com/apache/incubator-beam and build it)
* Java 8
* Maven

## Examples
* java -cp target/vortex-0.2-SNAPSHOT-shaded.jar beam.examples.MapReduce input output
* java -cp target/vortex-0.2-SNAPSHOT-shaded.jar beam.examples.Broadcast input output
