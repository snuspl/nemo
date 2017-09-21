# Vortex 
[![Build Status](https://cmsbuild.snu.ac.kr/buildStatus/icon?job=Vortex-master)](https://cmsbuild.snu.ac.kr/job/Vortex-master/)

## Vortex prerequisites and setup

### Prerequisites
* Java 8
* Maven
* Latest REEF snapshot
* Protobuf 2.5.0
    * Downloadable at https://github.com/google/protobuf/releases/tag/v2.5.0
    * On Ubuntu:
    1. Run `sudo apt-get install autoconf automake libtool curl make g++ unzip`
    2. Extract the downloaded tarball and run
        * `sudo ./configure`
        * `sudo make`
        * `sudo make check`
        * `sudo make install`
    3. To check for a successful installation of version 2.5.0, run `protoc --version`

### Installing Vortex
* Run all tests and install: `mvn clean install -T 2C`
* Run only unit tests and install: `mvn clean install -DskipITs -T 2C`

## Running Apache Beam applications
### Configurable options
* `-job_id`: ID of the Beam job
* `-user_main`: Beam application as a java class
* `-optimization_policy`: DAG optimizer applied in Vortex compiler.
Four policies are supported:
	* `DefaultPolicy`: No DAG optimization is applied.
	* `PadoPolicy` : Annotate DAG to perform optimizations of [EuroSys 2017 Pado](http://dl.acm.org/citation.cfm?id=3064181). This policy sets vertices that are likely to have high eviction cost to be located in `Reserved` resources and else in `Transient` resources so that eviction cost is minimized while preserving high system utilization.
	* `DisaggregationPolicy` : Annotate DAG that this job will run under disaggregated storage environment. For edges other than one-to-one, this policy sets data placement to `RemoteFile` and data transfer to `Pull` fashion so that it can utilize remote storages in the disaggregated storage environment.
	* `DataSkewPolicy` : Reshape the DAG to resolve data skew. This policy collects metrics to repartition the skewed data to evenly partitioned data.
* `-user_args`: locations of input and output files
* `-deploy_mode`:  `yarn` is supported

### Examples
```bash
## MapReduce Application
./bin/run.sh \
  -job_id mr_default \
  -user_main edu.snu.vortex.examples.beam.MapReduce \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.DefaultPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

## MapReduce Application using a separately defined pass
./bin/run.sh \
  -job_id mr_default \
  -user_main edu.snu.vortex.examples.beam.MapReduce \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.DefaultPolicyWithSeparatePass \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

## MapReduce Application with Pado optimization policy
./bin/run.sh 
  -job_id mr_pado \
  -user_main edu.snu.vortex.examples.beam.MapReduce \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.PadoPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

## MapReduce Application with Disaggregation optimization policy
./bin/run.sh \
  -job_id mr_disaggr \
  -user_main edu.snu.vortex.examples.beam.MapReduce \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.DisaggregationPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

## MapReduce Application with DataSkew dynamic optimization policy
./bin/run.sh \
  -job_id mr_dataskew \
  -user_main edu.snu.vortex.examples.beam.MapReduce \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.DataSkewPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

## Broadcast Application with Pado optimization policy
./bin/run.sh \
  -job_id broadcast_pado \
  -user_main edu.snu.vortex.examples.beam.Broadcast \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.PadoPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"

## Alternating Least Square Application with Pado optimization policy
./bin/run.sh \
  -job_id als_pado \
  -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.PadoPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_als 10 3"

## An inefficient Alternating Least Square Application with Pado optimization policy (to show optimizer functionalities)
./bin/run.sh \
  -job_id als_ineff_pado \
  -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquareInefficient \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.PadoPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_als 10 3"

## Multinomial Logistic Regression Application with Pado optimization policy
./bin/run.sh \
  -job_id mlr_pado \
  -user_main edu.snu.vortex.examples.beam.MultinomialLogisticRegression \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.PadoPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mlr 100 5 3"

## A simple toy example to demonstrate optimizer DAG transformation
java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.optimizer.examples.MapReduceDisaggregationOptimization

## YARN cluster example
./bin/run.sh \
  -deploy_mode yarn \
  -job_id mr_pado \
  -user_main edu.snu.vortex.examples.beam.MapReduce \
  -optimization_policy edu.snu.vortex.compiler.optimizer.policy.PadoPolicy \
  -user_args "hdfs://v-m:9000/sample_input_mr hdfs://v-m:9000/sample_output_mr"
```

## Resource Configuration
`-executor_json` command line option can be used to provide a path to the JSON file that describes resource configuration for executors. Its default value is `config/default.json`, which initializes one of each `Transient`, `Reserved`, and `Compute` executor, each of which has one core and 1024MB memory.

### Configurable options
* `num` (optional): Number of containers. Default value is 1
* `type`:  Three container types are supported:
	* `Transient` : Containers that store eviction-prone resources. When batch jobs use idle resources in `Transient` containers, they can be arbitrarily evicted when latency-critical jobs attempt to use the resources.
	* `Reserved` : Containers that store eviction-free resources. `Reserved` containers are used to reliably store intermediate data which have high eviction cost.
	* `Compute` : Containers that are mainly used for computation.
* `memory_mb`: Memory size in MB
* `capacity`: Number of `TaskGroup`s that can be run in an executor. Set this value to be the same as the number of CPU cores of the container.

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
Vortex Compiler and Engine can store JSON representation of intermediate DAGs.
`-dag_dir` command line option is used to specify the directory where the JSON files are stored. The default directory is `./target/dag`.
Using our [online visualizer](https://service.jangho.kr/vortex-dag/), you can easily visualize a DAG. Just drop the JSON file of the DAG as an input to it.

### Examples
```bash
./bin/run.sh \
    -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare \
    -optimization_policy pado \
    -dag_dir "./target/dag/als" \
    -user_args "`pwd`/src/main/resources/sample_input_als 10 3"
```