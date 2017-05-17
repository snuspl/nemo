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
package edu.snu.vortex.runtime.executor;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;

import javax.inject.Inject;
import java.util.logging.Logger;

@EvaluatorSide
@Unit
public final class VortexContext {

  private static final Logger LOG = Logger.getLogger(VortexContext.class.getName());

  @Inject
  private VortexContext(@Parameter(VortexAggregatorConf.NumOfThreads.class) final int numOfThreads) {
    // Set up executor and ncs here...
  }
}
