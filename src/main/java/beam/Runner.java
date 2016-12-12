package beam;

import dag.DAG;
import dag.DAGBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;

public class Runner extends PipelineRunner<Result> {

  public static PipelineRunner<Result> fromOptions(PipelineOptions options) {
    return new Runner();
  }

  public Result run(final Pipeline pipeline) {
    final DAGBuilder db = new DAGBuilder();
    final Visitor visitor = new Visitor(db);
    pipeline.traverseTopologically(visitor);
    final DAG dag = db.build();
    DAG.print(dag);
    return new Result();
  }
}
