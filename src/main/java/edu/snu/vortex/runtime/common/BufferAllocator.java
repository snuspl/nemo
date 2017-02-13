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
 * The interface that buffer allocators should implement.
 */
public interface BufferAllocator {

  /**
   * allocate a {@link ReadWriteBuffer} type buffer.
   * @return an allocated buffer.
   */
  ReadWriteBuffer allocateBuffer();

  /**
   * release a {@link ReadWriteBuffer} type buffer.
   * The module who implements this interface should clean up resources for the buffer properly.
   * @param buffer a buffer to be released.
   */
  void releaseBuffer(ReadWriteBuffer buffer);
}
