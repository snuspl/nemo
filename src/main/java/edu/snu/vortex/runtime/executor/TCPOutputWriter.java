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

import edu.snu.vortex.runtime.exception.NotImplementedException;

import java.util.List;

/**
 * An output writer which writes output data into MEMORY channels.
 * @param <T> The type of data records which are written into this output writer.
 */
public final class TCPOutputWriter<T> implements OutputWriter<T> {
  @Override
  public void writeOutputRecords(final List<T> records) {
    throw new NotImplementedException("This method has not been implemented.");
  }
}
