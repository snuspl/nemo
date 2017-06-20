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
package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.ir.Element;
import org.apache.reef.tang.annotations.Parameter;

import java.util.Optional;

/**
 * Store blocks in file.
 */
public final class FileStore implements BlockStore {

  public FileStore(@Parameter(JobConf.BlockDirectory.class) final String blockDirectory) {
  }

  @Override
  public Optional<Iterable<Element>> getBlock(final String blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBlock(final String blockId, final Iterable<Element> data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Iterable<Element>> removeBlock(final String blockId) {
    throw new UnsupportedOperationException();
  }
}
