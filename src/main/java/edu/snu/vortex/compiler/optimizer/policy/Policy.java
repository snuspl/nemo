/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.compiler.optimizer.policy;

import edu.snu.vortex.compiler.optimizer.pass.StaticOptimizationPass;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.Serializable;
import java.util.List;

/**
 * An interface for policies, each of which is composed of a list of static optimization passes.
 * The list of static optimization passes are run in the order provided by the implementation.
 */
@DefaultImplementation(DefaultPolicy.class)
public interface Policy extends Serializable {
  // TODO #479: Group, organize passes and update them accordingly to Policies.
  /**
   * @return the content of the policy: the list of static optimization passes of the policy.
   */
  List<StaticOptimizationPass> getOptimizationPasses();
}