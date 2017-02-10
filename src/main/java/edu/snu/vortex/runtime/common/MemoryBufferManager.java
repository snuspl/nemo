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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemoryBufferManager implements BufferAllocator {
  private final Set<Integer> freeMemBufferIdSet;
  private final Set<Integer> usedMemBufferIdSet;
  private final Map<Integer, MemoryBuffer> freeMemBufferMap;
  private final Map<Integer, MemoryBuffer> usedMemBufferMap;

  //TODO: (possible improvement) change to create memory buffers on demand rather than pre-allocate.
    MemoryBufferManager(int numMemoryBuffer, int memoryBufferSize) {
    final AtomicInteger idFactory = new AtomicInteger(0);
    freeMemBufferIdSet = new HashSet<>();
    usedMemBufferIdSet = new HashSet<>();
    freeMemBufferMap = new HashMap<>();
    usedMemBufferMap = new HashMap<>();

    for (int i = 0; i < numMemoryBuffer; i++) {
      final int bufferId = idFactory.getAndIncrement();
      freeMemBufferMap.put(bufferId, new MemoryBuffer(bufferId,
                                                      ByteBuffer.allocate(memoryBufferSize),
                                                      memoryBufferSize));
      freeMemBufferIdSet.add(bufferId);
    }
  }

  public MemoryBuffer allocateBuffer() {
    final Integer freeBufferId = freeMemBufferIdSet.stream().findFirst().get();
    if (freeBufferId == null) {
      throw new OutOfMemoryError("no memory buffer available");
    }

    final MemoryBuffer memoryBuffer = freeMemBufferMap.remove(freeBufferId);
    freeMemBufferIdSet.remove(freeBufferId);

    usedMemBufferMap.put(freeBufferId, memoryBuffer);
    usedMemBufferIdSet.add(freeBufferId);

    return memoryBuffer;
  }

  public void releaseBuffer(final ReadWriteBuffer buffer) {
    releaseBuffer((MemoryBuffer) buffer);
  }

  private void releaseBuffer(final MemoryBuffer memoryBuffer) {
    final Integer bufferId = memoryBuffer.getId();

    usedMemBufferMap.remove(bufferId);
    usedMemBufferIdSet.remove(bufferId);

    memoryBuffer.clear();
    freeMemBufferMap.put(bufferId, memoryBuffer);
    freeMemBufferIdSet.add(bufferId);
  }
}
