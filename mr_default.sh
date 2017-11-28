rm -rf REEF_LOCAL_RUNTIME/*
./bin/run.sh \
  -job_id mr_default \
  -user_main edu.snu.onyx.examples.beam.MapReduce \
  -optimization_policy edu.snu.onyx.compiler.optimizer.policy.DefaultPolicy \
  -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/outputs/sample_output_" \
  -dag_dir "`pwd`/dag/mr_default"
