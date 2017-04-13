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
package edu.snu.vortex.examples.beam;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class ArgGen {
  public static List<String> genUserMain(final String main) {
    return Arrays.asList("-user_main", main);
  }

  public static List<String> genUserArgs(final String... args) {
    final StringJoiner joiner = new StringJoiner(" ");
    Arrays.stream(args).forEach(joiner::add);
    return Arrays.asList("-user_args", joiner.toString());
  }

  public static List<String> genOptimizationPolicy(final String policy) {
    return Arrays.asList("-optimization_policy", policy);
  }

  public static String[] getFinalArgs(final List<String>... args) {
    // new String[0] is good for performance
    // see http://stackoverflow.com/questions/4042434/converting-arrayliststring-to-string-in-java
    return Arrays.stream(args).flatMap(List::stream).collect(Collectors.toList()).toArray(new String[0]);
  }
}
