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
import java.util.ArrayList;
import java.util.List;

public final class LocalFileManager {
  private final static String ROOT_DIR_NAME = "filemgr";
  private final List<File> rootDirs;
  private final List<List<File>> subDirs;
  private final int numSubDirsPerRootDir;

  // TODO: change to get the list of file spaces not as parameter but within an executor configuration
  LocalFileManager(List<File> fileSpaces, int numSubDirsPerRootDir) {
    this.rootDirs = new ArrayList<>(fileSpaces.size());
    this.subDirs = new ArrayList<>(fileSpaces.size());
    this.numSubDirsPerRootDir = numSubDirsPerRootDir;
    for (int i = 0; i < fileSpaces.size(); i++) {
      this.subDirs.add(new ArrayList<>(numSubDirsPerRootDir));
    }

    fileSpaces.forEach(fs -> this.rootDirs.add(new File(fs, ROOT_DIR_NAME)));
  }

  public File getFileByName(String fileName) throws IOException {
    final int hashCode = Math.abs(fileName.hashCode());
    final int rootDirIdx = hashCode % rootDirs.size();
    final int subDirIdx = (hashCode / rootDirs.size()) % numSubDirsPerRootDir;
    File file;

    synchronized (subDirs.get(rootDirIdx)) {
      final File subDir = subDirs.get(rootDirIdx).get(subDirIdx);
      if (subDir != null) {
        file = new File(subDir, fileName);
      } else {
        final File newDir = new File(rootDirs.get(rootDirIdx), String.format("%02x", subDirIdx));
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException("failed to create a directory in " + newDir);
        }
        subDirs.get(rootDirIdx).set(subDirIdx, newDir);
        file = new File(newDir, fileName);
      }
    }

    return file;
  }

}
