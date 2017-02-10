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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class LocalFileBufferManager implements BufferAllocator {
  private static final String LOCAL_FILE_PREFIX = "local-file-buffer-manager-";
  private final LocalFileManager fileManager;
  private final AtomicInteger idFactory = new AtomicInteger(0);
  private final Map<Integer, File> bufferIdToFileMap;

  LocalFileBufferManager(LocalFileManager fileManager) {
    this.fileManager = fileManager;
    bufferIdToFileMap = new HashMap<>();
  }

  public ReadWriteBuffer allocateBuffer() {
    final int bufferId = idFactory.getAndIncrement();
    final String fileName = LOCAL_FILE_PREFIX + bufferId;
    try {
      final File file = fileManager.getFileByName(fileName);
      final LocalFileBuffer buffer = new LocalFileBuffer(bufferId, file);
      bufferIdToFileMap.put(bufferId, file);

      return buffer;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void releaseBuffer(ReadWriteBuffer fileBuffer) {
    final int bufferId = fileBuffer.getId();
    bufferIdToFileMap.remove(bufferId).delete();
  }
}
