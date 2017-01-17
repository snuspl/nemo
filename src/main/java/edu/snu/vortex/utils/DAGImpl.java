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
package edu.snu.vortex.utils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implements DAG with adjacent list.
 * It is based on the one of MIST.
 * @param <V> type of the vertex
 */
@NotThreadSafe
public final class DAGImpl<V> implements DAG<V> {
  private static final Logger LOG = Logger.getLogger(DAGImpl.class.getName());
  /**
   * A set of root vertices.
   */
  private final Set<V> rootVertices = new HashSet<>();

  /**
   * A map containing <vertex, Set of its parent vertices> mappings.
   */
  private final Map<V, Set<V>> parentVertices = new HashMap<>();

  /**
   * A map containing <vertex, Set of its children vertices> mappings.
   */
  private final Map<V, Set<V>> childrenVertices = new HashMap<>();

  /**
   * A map containing <vertex, Set of its Connected Components' root vertices> mappings.
   */
  private final Map<V, List<V>> connectedComponentMap = new HashMap<>();

  @Override
  public Set<V> getRootVertices() {
    return Collections.unmodifiableSet(rootVertices);
  }

  @Override
  public boolean isAdjacent(final V v, final V w) {
    throw new UnsupportedOperationException("Needs to be implemented");
  }

  @Override
  public Set<V> getNeighbors(final V v) {
    throw new UnsupportedOperationException("Needs to be implemented");
  }

  @Override
  public boolean addVertex(final V v) {
    if (!childrenVertices.containsKey(v) && !parentVertices.containsKey(v)) {
      childrenVertices.put(v, new HashSet<>());
      parentVertices.put(v, new HashSet<>());
      rootVertices.add(v);

      final List<V> connectedRootVertices = new LinkedList<>();
      connectedRootVertices.add(v);
      connectedComponentMap.put(v, connectedRootVertices);
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} already exists", v);
      return false;
    }
  }

  @Override
  public boolean removeVertex(final V v) {
    final Set<V> children = childrenVertices.remove(v);
    if (children != null) {
      for (final V child : children) {
        final Set<V> parents = parentVertices.get(child);
        parents.remove(v);
        final List<V> rootsToRemove = connectedComponentMap.get(v);
        if (parents.isEmpty()) {
          rootVertices.add(child);
          updateConnectedComponentMap(child, rootsToRemove, child);
        }
      }
      rootVertices.remove(v);
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} does exists", v);
      return false;
    }
  }

  private void updateConnectedComponentMap(final V v, final List<V> rootsToRemove, final V rootToAdd) {
    final List<V> roots = connectedComponentMap.get(v);
    rootsToRemove.forEach(root -> roots.remove(root));
    roots.add(rootToAdd);

    final Set<V> children = childrenVertices.get(v);
    children.forEach(child -> updateConnectedComponentMap(child, rootsToRemove, rootToAdd));
  }

  @Override
  public boolean addEdge(final V src, final V dst) {
    if (!childrenVertices.containsKey(src) || !parentVertices.containsKey(src)) {
      throw new NoSuchElementException("No src vertex " + src);
    }
    if (!childrenVertices.containsKey(dst) || !parentVertices.containsKey(dst)) {
      throw new NoSuchElementException("No dest vertex " + dst);
    }

    final Set<V> children = childrenVertices.get(src);
    final List<V> srcRoots = connectedComponentMap.get(src);
    final List<V> dstRoots = connectedComponentMap.get(dst);

    srcRoots.forEach(root -> {
      if (dstRoots.contains(root)) {
        throw new IllegalStateException("The edge from " + src + " to " + dst + " makes a cycle in the graph");
      }});

    if (connectedComponentMap.get(src).equals(connectedComponentMap.get(dst))) {
      throw new IllegalStateException("The edge from " + src + " to " + dst + " makes a cycle in the graph");
    }

    if (children.add(dst)) {
      final int inDegree = 0; //inDegrees.get(dst);
      if (inDegree == 0) {
//        if (rootVertices.size() == 1) {
//          throw new IllegalStateException("The edge from " + src + " to " + dst + " makes a cycle in the graph");
//        }
        rootVertices.remove(dst);
      }
//      inDegrees.put(dst, inDegree + 1);
      connectedComponentMap.put(dst, connectedComponentMap.get(src));
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} already exists", new Object[]{src, dst});
      return false;
    }
  }

  @Override
  public boolean removeEdge(final V v, final V w) {
    final Set<V> adjs = null; //adjacent.get(v);
    if (adjs == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }

    if (adjs.remove(w)) {
      final int inDegree = 0; //inDegrees.get(w);
//      inDegrees.put(w, inDegree - 1);
      if (inDegree == 1) {
        rootVertices.add(w);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} does not exists", new Object[]{v, w});
      return false;
    }
  }

  @Override
  public int getInDegree(final V v) {
    throw new UnsupportedOperationException("Needs to be implemented");
  }
}
