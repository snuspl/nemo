package dag;
/*
 * Copyright (C) 2016 Seoul National University
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

import java.util.HashMap;

public class SourceNode<O> implements Node<Void, O> {
  private final String id;
  private final HashMap<String, Object> attributes;
  private final Source<O> source;

  SourceNode(final Source<O> source) {
    this.id = IdManager.newNodeId();
    this.attributes = new HashMap<>();
    this.source = source;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public HashMap<String, Object> getAttributes() {
    return attributes;
  }

  public Source<O> getSource() {
    return source;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", source: ");
    sb.append(source);
    return sb.toString();
  }

}
