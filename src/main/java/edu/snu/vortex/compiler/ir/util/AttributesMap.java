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
package edu.snu.vortex.compiler.ir.util;

import edu.snu.vortex.compiler.ir.Attributes;

import java.util.HashMap;

/**
 * AttributesMap Class, which uses HashMap for keeping track of attributes for operators and edges.
 */
public final class AttributesMap extends HashMap<Attributes.Key, Attributes> {
  public Attributes put(final Attributes.Key key, final Attributes val) {
    if (!val.hasKey(key)) {
      throw new RuntimeException("Attribute " + val + "is not a member of Key " + key);
    }
    return super.put(key, val);
  }
}
