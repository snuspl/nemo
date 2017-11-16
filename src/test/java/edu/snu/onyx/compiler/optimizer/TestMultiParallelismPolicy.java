package edu.snu.onyx.compiler.optimizer;

import edu.snu.onyx.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.ParallelismPass;
import edu.snu.onyx.compiler.optimizer.pass.runtime.RuntimePass;
import edu.snu.onyx.compiler.optimizer.policy.Policy;

import java.util.ArrayList;
import java.util.List;

/**
 * A policy to test multi-parallelism. ONLY FOR TESTING!
 */
public class TestMultiParallelismPolicy implements Policy {
  private Policy testPolicy = new TestPolicy();

  @Override
  public List<CompileTimePass> getCompileTimePasses() {
    final List<CompileTimePass> list = new ArrayList<>();
    list.add(new ParallelismPass(3));
    list.addAll(testPolicy.getCompileTimePasses());
    return list;
  }

  @Override
  public List<RuntimePass<?>> getRuntimePasses() {
    return new ArrayList<>();
  }
}
