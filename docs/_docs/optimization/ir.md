---
title: Onyx IR
permalink: /docs/ir/
---

### Overview

IR is an abstraction that we use to express the logical notion of data processing applications and the underlying execution runtime behaviors on separate layers.
It basically takes a form of directed acyclic graphs (DAGs), with which we can logically express dataflow programs.
To express various different execution properties to fully exploit different deployment characteristics, we enable flexible annotations to the IR on a separate layer.
On the layer, we can annotate specific execution properties related to the IR component.

### IR structure

Onyx IR is composed of vertices, which each represent a data-parallel operator that transforms data, and edges between them, which each represents the dependency of data flow between the vertices.
Onyx IR supports four different types of IR vertices:

- UDF Vertex: Most commonly used vertex. Each UDF vertex contains a transform which determines the actions to take for the given input data. Transform can express any kind of data processing operation that high-level languages articulate
- Source Vertex: 
- Metric Vertex: 
- Loop Vertex: 



As this vertex is capable of expressing any kind of an arbitrary \textit{transform}, it can express any kind of data processing operation that high-level languages articulate.
Next, as the name suggests, a source vertex produces data by reading from an arbitrary source, like disks and distributed filesystems, and a metric vertex collects and emits metric data.
A loop vertex is used to express iterative workflows, summarizing the part of the IR that occur repetitively due to iterations.
This part comes very useful when expressing, controlling and optimizing iterative workloads like multinomial logistic regression.
With a loop vertex, the IR becomes much simpler and easier to optimize, and it can manage iterative operations not only by a definitive number of iterations, but also by a condition on which it stops iterating.

Each of the IR vertices and IR edges can be annotated to be able to express the different \textit{execution properties} that have been mentioned earlier (\S\ref{sec:execution_properties}).
For example, edges that the user wants to store the intermediate data as local file can be annotated to use the 'local file' module for the 'Data Store' execution property.
\textit{Execution properties} that can be configured for IR vertices include parallelism, executor placement, stage, and schedule group, as they are related to the computation itself.
For IR edges, it includes data store, data flow model, data communication pattern, and partitioning, as they are used for expressing the behaviors regarding data transfer.
By having a IR for expressing workloads and the related execution properties, it enables the optimization phase to be decoupled, making it easier to implement and plug in different optimizations for different \textit{deployment characteristics}, which we discuss in the subsection below.
