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

import edu.snu.vortex.compiler.exception.CompileTimeOptimizationException;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.vortex.compiler.optimizer.pass.runtime.RuntimePass;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A builder for policies.
 */
public final class PolicyBuilder {
  private final List<CompileTimePass> compileTimePasses;
  private final List<RuntimePass<?>> runtimePasses;
  private final Set<ExecutionProperty.Key> requiredExecutionProperties;
  private final Set<ExecutionProperty.Key> annotatedExecutionProperties;

  public PolicyBuilder() {
    this.compileTimePasses = new ArrayList<>();
    this.runtimePasses = new ArrayList<>();
    this.requiredExecutionProperties = new HashSet<>();
    this.annotatedExecutionProperties = new HashSet<>();
  }

  public PolicyBuilder(final JSONObject jsonObject) throws Exception {
    this();
    JSONArray commands = (JSONArray) jsonObject.get("policy");
    for (int i = 0; i < commands.size(); i++) {
      JSONObject command = (JSONObject) commands.get(i);
      String type = (String) command.get("type");
      switch (type) {
        case "CompileTimePass":
          registerCompileTimePass((CompileTimePass) Class.forName((String) command.get("name")).newInstance());
          break;
        case "RuntimePass":
          final JSONArray namePair = (JSONArray) command.get("names");
          registerRuntimePass((RuntimePass<?>) Class.forName((String) namePair.get(0)).newInstance(),
              (CompileTimePass) Class.forName((String) namePair.get(1)).newInstance());
          break;
        case "ExecutionProperty":
          addExecutionPropertyRequirement(ExecutionProperty.Key.valueOf((String) command.get("key")));
        default:
          throw new CompileTimeOptimizationException("There is no such pass type: " + type);
      }
    }
  }

  public PolicyBuilder registerCompileTimePass(final CompileTimePass compileTimePass) {
    // Check prerequisite execution properties.
    if (!annotatedExecutionProperties.containsAll(compileTimePass.getPrerequisiteExecutionProperties())) {
      throw new CompileTimeOptimizationException("Prerequisite ExecutionProperty hasn't been met for "
          + compileTimePass.getName());
    }

    if (compileTimePass instanceof AnnotatingPass) {
      final AnnotatingPass annotatingPass = (AnnotatingPass) compileTimePass;
      this.annotatedExecutionProperties.add(annotatingPass.getExecutionPropertyToModify());
    }
    this.compileTimePasses.add(compileTimePass);
    return this;
  }

  public PolicyBuilder registerRuntimePass(final RuntimePass<?> runtimePass, final CompileTimePass compileTimePass) {
    registerCompileTimePass(compileTimePass);
    this.runtimePasses.add(runtimePass);
    return this;
  }

  public PolicyBuilder addExecutionPropertyRequirement(final ExecutionProperty.Key key) {
    requiredExecutionProperties.add(key);
    return this;
  }

  public Policy build() {
    // see if required execution properties have been met
    if (!annotatedExecutionProperties.containsAll(requiredExecutionProperties)) {
      throw new CompileTimeOptimizationException("Required execution properties has not been met for the policy");
    }

    return new Policy() {
      @Override
      public List<CompileTimePass> getCompileTimePasses() {
        return compileTimePasses;
      }

      @Override
      public List<RuntimePass<?>> getRuntimePasses() {
        return runtimePasses;
      }
    };
  }
}
