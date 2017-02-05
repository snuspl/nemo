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

import java.io.*;

public final class LocalFileIOBuffer implements DataBuffer {

  private final int bufferId;
  private final File file;
  private final long fileMaxSize;
  private long fileSize;
  private long fileSeek;
  private BufferedInputStream inputStream;
  private BufferedOutputStream outputStream;


  LocalFileIOBuffer(final int bufferId, final File file, final long fileMaxSize) throws FileNotFoundException {
    this.bufferId = bufferId;
    this.file = file;
    this.fileMaxSize = fileMaxSize;
    this.fileSize = 0;
    this.inputStream = new BufferedInputStream(new FileInputStream(file));
    this.outputStream = new BufferedOutputStream(new FileOutputStream(file));
  }

  LocalFileIOBuffer(final int bufferId, final File file) throws FileNotFoundException {
    this(bufferId, file, Long.MAX_VALUE);
  }

  @Override
  public int getId() {
    return bufferId;
  }

  @Override
  public int writeNext(final byte[] data, final int bufSizeInByte) {
    final int writeDataSize = (bufSizeInByte < (fileMaxSize - fileSize))? bufSizeInByte: (int)(fileMaxSize - fileSize);

    try {
      outputStream.write(data, 0, writeDataSize);
    } catch(IOException e) {
      throw new RuntimeException(e);
    }

    fileSize += writeDataSize;
    return writeDataSize;
  }

  @Override
  public int readNext(final byte[] readBuffer, final int bufSizeInByte) {
    final int readDataSize = (bufSizeInByte < (fileSize - fileSeek))? bufSizeInByte: (int) (fileSize - fileSeek);

    try {
      inputStream.read(readBuffer, 0, readDataSize);
    } catch(IOException e) {
      throw new RuntimeException(e);
    }

    fileSeek += readDataSize;
    return readDataSize;
  }

  @Override
  public void seekFirst() {
    try {
      inputStream.close();
      flush();

      inputStream = new BufferedInputStream(new FileInputStream(file));
      fileSeek = 0;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getBufferSize() {
    return fileSize;
  }

  @Override
  public long getRemainingDataSize() {
    return fileSize - fileSeek;
  }

  @Override
  public void flush() {
    try {
      outputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      outputStream.close();
      inputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
