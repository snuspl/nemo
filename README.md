# Vortex 
[![Build Status](https://cmsbuild.snu.ac.kr/buildStatus/icon?job=Vortex-master)](https://cmsbuild.snu.ac.kr/job/Vortex-master/)

## Vortex prerequisites and setup

### Prerequisites
* Java 8
* Maven
* Latest REEF snapshot
* Protobuf 2.5.0

-- Downloadable at https://github.com/google/protobuf/releases/tag/v2.5.0

-- On Ubuntu:
    1. Run `sudo apt-get install autoconf automake libtool curl make g++ unzip`
    2. Extract the downloaded tarball and run
        `sudo ./configure`
        `sudo make`
        `sudo make check`
        `sudo make install`
    3. To check for a successful installation of version 2.5.0, run `protoc --version`

### Installing Vortex
* Run all tests and install: `mvn clean install -T 2C`
* Run only unit tests and install: `mvn clean install -DskipITs -T 2C`

## Running Apache Beam applications
### Configurable options
* `-job_id`: ID of the Beam job
* `_user_main`: Beam application as a java class
* `-optimization_policy`: DAG optimizer applied in Vortex compiler. `default`, `pado`, `disaggregation`, `dataskew` are supported
* `-user_args`: locations of input and output files
* `-deploy_mode`:  `yarn` is supported

### Examples
```bash
./bin/run.sh \
    -job_id mr_default \
    -user_main edu.snu.vortex.examples.beam.MapReduce \
    -optimization_policy default \
    -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

./bin/run.sh \
    -job_id mr_pado \
    -user_main edu.snu.vortex.examples.beam.MapReduce \
    -optimization_policy pado \
    -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

./bin/run.sh \
    -job_id mr_disaggr \
    -user_main edu.snu.vortex.examples.beam.MapReduce \
    -optimization_policy disaggregation \
    -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

./bin/run.sh \
    -job_id mr_dataskew \
    -user_main edu.snu.vortex.examples.beam.MapReduce \
    -optimization_policy dataskew \
    -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

./bin/run.sh \
    -job_id broadcast_pado \
    -user_main edu.snu.vortex.examples.beam.Broadcast \
    -optimization_policy pado \
    -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

./bin/run.sh \
    -job_id als_pado \
    -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare \
    -optimization_policy pado \
    -user_args "`pwd`/src/main/resources/sample_input_als 10 3"

./bin/run.sh \
    -job_id als_ineff_pado \
    -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquareInefficient \
    -optimization_policy pado \
    -user_args "`pwd`/src/main/resources/sample_input_als 10 3"

./bin/run.sh \
    -job_id mlr_pado \
    -user_main edu.snu.vortex.examples.beam.MultinomialLogisticRegression\
    -optimization_policy pado \
    -user_args "`pwd`/src/main/resources/sample_input_mlr 100 5 3"

java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.optimizer.examples.MapReduce

## YARN cluster example
./bin/run.sh \
    -deploy_mode yarn \
    -job_id mr_pado \
    -user_main edu.snu.vortex.examples.beam.MapReduce \
    -optimization_policy pado \
    -user_args "hdfs://v-m:9000/sample_input_mr hdfs://v-m:9000/sample_output_mr"
```

## Resource Configuration
`-executor_json` command line option can be used to provide a path to the JSON file that describes resource configuration for executors. Its default value is `config/default.json`, which initializes one of each `Transient`, `Reserved`, and `Compute` executor, each of which has one core and 1024MB memory.

### Configurable options
* `num` (optional): Number of containers. Default value is 1
* `type`: `Transient`, `Reserved`, `Compute`
* `memory_mb`: Memory size in MB
* `capacity`: Number of `TaskGroup`s that can be run in an executor. We define this value to be identical to the number of CPU cores of the container.

### Examples
```json
[
  {
    "num": 12,
    "type": "Transient",
    "memory_mb": 1024,
    "capacity": 4
  },
  {
    "type": "Reserved",
    "memory_mb": 1024,
    "capacity": 2
  }
]
```

This example configuration specifies
* 12 transient containers with 4 cores and 1024MB memory each
* 1 reserved container with 2 cores and 1024MB memory

## Monitoring your job using web UI
Vortex Compiler and Engine stores JSON representation of intermediate DAGs.
`-dag_dir` command line option specifies the directory to store the JSON files. By default the JSON files are saved in `./target/dag`.
You can easily visualize a DAG using our [online visualizer](https://service.jangho.kr/vortex-dag/) by dropping the corresponding JSON file to it.

### Examples
```bash
./bin/run.sh \
    -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare \
    -optimization_policy pado \
    -dag_dir "./target/dag/als" \
    -user_args "`pwd`/src/main/resources/sample_input_als 10 3"
```




