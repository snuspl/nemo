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

import edu.snu.vortex.runtime.common.DataBuffer;
import edu.snu.vortex.runtime.common.BlockInfo;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Executor-side Block Manager.
 */
public final class BlockManager {

  public String createLocalBlock(final DataBuffer data) {
    throw new NotImplementedException();
  }

  public String createRemoteBlockAsCheckpointSnapShot(final DataBuffer data) {
    throw new NotImplementedException();
  }

  public boolean isLocal(final String blockId) {
    throw new NotImplementedException();
  }

  public boolean removeLocalBlock(final String blockId) {
    throw new NotImplementedException();
  }

  public DataBuffer getBlockData(final String blockId) {
    throw new NotImplementedException();
  }

  public boolean pullRemoteBlock(final String blockId) {
    throw new NotImplementedException();
  }

  public BlockInfo getBlockInfo(final String blockId) {
    throw new NotImplementedException();
  }
}
