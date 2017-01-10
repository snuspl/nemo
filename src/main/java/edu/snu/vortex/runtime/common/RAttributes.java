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

public class RAttributes {
  public enum ROpAttribute {PARALLELISM, PARTITION, RESOURCE_TYPE}
  public enum Partition {HASH, RANGE}
  public enum Resource_Type {TRANSIENT, RESERVED, COMPUTE, STORAGE}

  public enum ROpLinkAttribute {CHANNEL, COMM_PATTERN}
  public enum Channel {LOCAL_MEM, TCP, FILE, DISTR_STORAGE}
  public enum Comm_Pattern {ONE_TO_ONE, BROADCAST, SCATTER_GATHER}
}
