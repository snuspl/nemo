---
title: Compiler Design
permalink: /docs/compiler_design/
---

### Overview

Compiler takes as input a dataflow program, and outputs an optimized physical execution plan to be understood by the execution runtime. 
Compiler first translates the logical layer of given dataflow programs written in high-level languages, like Apache BEAM, into expressive, general-purpose intermediate representations (IR), called \onyx IR.
Then, using our \textit{optimization pass} interface provided by the Compiler optimizer, the IR can be flexibly reshaped and annotated with a variety of \textit{execution properties} that configures the underlying runtime behaviors.
After being processed by optimization passes, the Compiler backend finally lays out the IR into a physical execution plan, composed of tasks and stages, to be carried out by the \onyx Execution Runtime.

### Frontend

The translator of \onyx Compiler translates arbitrary high-level dataflow languages, like Apache Beam, into our expression of \onyx IR with an elementary annotation of default \textit{execution properties}.
Translators are designed as visitors that traverse given applications written in high-level dataflow languages in a topological order.
While traversing the logic, it translates each dataflow operators and edges on the way, and appends the translated IR components to the \onyx IR builder.
After completing the traversal, the IR builder builds the logical part of the IR after checking its integrity.
Integrity check ensures a few factors, such as that vertices without any incoming edges read source data and those without any outgoing edges perform sink operations.

### Optimizer

After the IR is created with its logical structures set up, we need an \onyx policy to optimize the application for a specific goal.
To build \onyx policies safely and correctly, we provide a \textit{policy builder} interface, which checks for the integrity while registering a series of passes in a specific order.
For example, if an annotating pass requires information of specific \textit{execution properties} to perform its work, we specify them as 'prerequisite execution properties', and checks the previously registered passes to ensure that the conditions have been met.
We avoid the cases where circular dependencies occur, through the elementary default execution properties that we provide at the initiation of the \onyx IR.

Using the policy, the optimizer applies each \textit{optimization passes} one-by-one in the provided order, and checks for the IR integrity after each optimization has been done, to ensure that the IR is not broken.
We later explore examples of the results of optimizations done through \textit{policies} made up of complete series of \textit{optimization passes} in section~\ref{sec:examples}, with real-world applications.

### Backend

After the optimizations have been applied, Compiler backend finally traverses and lays out the IR into a physical execution plan, which is understood by Execution Runtime.
In the backend, vertices that annotated with the same stage numbers are grouped into stages, to be concurrently run in a distributed manner, and are expressed in the form of tasks.
The generated physical execution plan composed of tasks, task groups (stages), and the data dependency information between them is then submitted to Execution Runtime to be scheduled and executed.
