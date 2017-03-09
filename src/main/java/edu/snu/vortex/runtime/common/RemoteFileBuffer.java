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
package edu.snu.vortex.runtime.common;

import edu.snu.vortex.runtime.exception.NotImplementedException;

// TODO #000: implement ReadWriteBuffer interfaces.

/**
 * A buffer backed by remote file managed in distributed filesystem.
 */
public final class RemoteFileBuffer implements ReadWriteBuffer {
  @Override
  public int getId() {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public int writeNext(final byte[] data, final int bufSizeInByte) {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public int readNext(final byte[] readBuffer, final int bufSizeInByte) {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public void seekFirst() {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public long getBufferSize() {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public long getRemainingDataSize() {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public void flush() {
    throw new NotImplementedException("This method has not been implemented yet.");
  }

  @Override
  public void clear() {
    throw new NotImplementedException("This method has not been implemented yet.");
  }
}
