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

/**
 *
 */
public interface ReadWriteBuffer {
  /**
   * @return the buffer id.
   */
  int getId();

  /**
   * append byte data to the end of the buffer.
   * @param data the data to write.
   * @param bufSizeInByte the size of data to write in byte.
   * @return the actual size of data written in the buffer.
   */
  int writeNext(byte[] data, int bufSizeInByte);

  /**
   * read byte data starting from the seek point of the buffer.
   * @param readBuffer the buffer to store data.
   * @param bufSizeInByte the size of the read buffer.
   * @return the actual size of data read from the buffer.
   */
  int readNext(byte[] readBuffer, int bufSizeInByte);

  /**
   * set data seek to point the beginning of the buffer.
   */
  void seekFirst();

  /**
   * @return the total size of the buffer.
   */
  long getBufferSize();

  /**
   * @return the size of remaining data to be read.
   */
  long getRemainingDataSize();

  /**
   * actually store cached data into the buffer.
   */
  void flush();

  /**
   * reset the buffer to the initial state.
   */
  void clear();
}
