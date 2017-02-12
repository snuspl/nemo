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


public final class DataBuffer {
  private final String bufferId;
  private final DataBufferType bufferType;
  private final ReadWriteBuffer buffer;

  DataBuffer (final String bufferId, final DataBufferType bufferType, final ReadWriteBuffer buffer) {
    this.bufferId = bufferId;
    this.bufferType = bufferType;
    this.buffer = buffer;
  }

  public String getId() {
    return bufferId;
  }

  public DataBufferType getType() {
    return bufferType;
  }

  public int writeNext(final byte [] data, final int bufSizeInByte) {
    return buffer.writeNext(data, bufSizeInByte);
  }

  public int readNext(final byte [] readBuffer, final int bufSizeInByte) {
    return buffer.readNext(readBuffer, bufSizeInByte);
  }

  public void seekFirst() {
    buffer.seekFirst();
  }

  public long getBufferSize() {
    return buffer.getBufferSize();
  }

  public long getRemainingDataSize() {
    return buffer.getRemainingDataSize();
  }

  public void flush() {
    buffer.flush();
  }

  public void clear() {
    buffer.clear();
  }
}
