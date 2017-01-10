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

import java.io.Serializable;
import java.util.Map;

public class ROperator<I, O> implements Serializable{
  private final String rOpId;
  private final Map<RAttributes.ROpAttribute, Object> attributes;

  public ROperator(final String rOpId, final Map<RAttributes.ROpAttribute, Object> attributes) {
    this.rOpId = rOpId;
    this.attributes = attributes;
  }

  public void addAttrbute(final RAttributes.ROpAttribute key, final Object value) {
    attributes.put(key, value);
  }

  public void removeAttrbute(final RAttributes.ROpAttribute key) {
    attributes.remove(key);
  }

  public Map<RAttributes.ROpAttribute, Object> getAttributes() {
    return attributes;
  }
}
