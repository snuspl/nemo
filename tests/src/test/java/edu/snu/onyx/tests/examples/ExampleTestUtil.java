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
package edu.snu.onyx.tests.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class ExampleTestUtil {
  public static boolean isOutputValid(String resourcePath, String outputFileName, String testResourceFileName) throws IOException {
    String testOutput = Files.list(Paths.get(resourcePath))
        .filter(Files::isRegularFile)
        .filter(path -> path.getFileName().toString().startsWith(outputFileName))
        .flatMap(path -> {
          try {
            return Files.lines(path);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        })
        .sorted()
        .reduce("", (p, q) -> (p + "\n" + q));

    String resourceOutput = Files.lines(Paths.get(resourcePath + testResourceFileName))
        .sorted()
        .reduce("", (p, q) -> (p + "\n" + q));

    return testOutput.equals(resourceOutput);
  }
}