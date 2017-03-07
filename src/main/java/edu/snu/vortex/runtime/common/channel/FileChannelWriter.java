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
package edu.snu.vortex.runtime.common.channel;

/**
 * A file channel writer implementation.
 * @param <T> The type of data records that transfer via this channel.
 */
public class FileChannelWriter<T> implements ChannelWriter<T> {
  @Override
  public void write(final Iterable<T> data) {

  }

  @Override
  public void commit() {

  }

  @Override
  public String getId() {
    return null;
  }

  @Override
  public ChannelType getType() {
    return null;
  }

  @Override
  public ChannelMode getMode() {
    return null;
  }

  @Override
  public String getSrcTaskId() {
    return null;
  }

  @Override
  public String getDstTaskId() {
    return null;
  }

  @Override
  public void setDstTaskId(final String newDstTaskId) {

  }

  @Override
  public void initialize() {

  }
}
