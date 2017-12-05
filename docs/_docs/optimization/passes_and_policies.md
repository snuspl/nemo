---
title: Passes and Policies
permalink: /docs/passes_and_policies/
---

### Overview

% % optimization pass intro
The \onyx IR can be flexibly modified, both in its logical structure and annotations, through an interface called \textit{\onyx optimization pass}.
Simply put, an \textit{optimization pass} is a function that takes an \onyx IR and outputs an optimized \onyx IR.
The modification during compile-time occurs in two representative ways: through \textit{reshaping passes} and \textit{annotating passes}.
First, \textit{reshaping passes} modify the shape of the IR itself by inserting, regrouping, or deleting IR vertices and edges on an \onyx IR, like collecting repetitive vertices inside a single loop.
Second, \textit{annotating passes} annotates IR vertices and edges with specific \textit{execution properties} with the given logic to adjust and run the workload in the fashion that the user wants.
Such optimization passes can be grouped together as a \textit{composite pass} for convenience.

% Example passes
Specifically, as shown in Table~\ref{table:passes} a reshaping pass can be utilized to perform various IR optimizations including common subexpression elimination (CSE), dead code elimination, and loop invariant code motion, by observing the structure of the IR and performing optimizations during compile-time.
It can also detect repetitive patterns inside an IR, and extract and express that part of the IR as a loop.
Furthermore, an annotating pass can determine computational parallelism of each vertices by observing the source data size and the parallelism information of previous vertices, and allocate different computations on specific types of resources.

% % Some more functionalities of optimization passes: flexibility
Furthermore, an \textit{optimization pass} can also be performed during runtime to perform dynamic optimizations, such as data skew, using runtime statistics.
This action occurs dynamically after the \onyx IR has been submitted to the execution runtime, after going through compile-time passes and being laid out as a physical execution plan.
It receives and modifies the execution plan using the given metric data of runtime statistics, so that the execution is dynamically optimized during runtime using the provided optimization logic specified by the user.

% % Optimization policy is composed of a combination of optimization passes: flexibility + extensibility
Using a carefully chosen series of \textit{optimization passes}, we can optimize an application to exploit specific \textit{deployment characteristics}, by providing appropriate configurations for the execution runtime.
A complete series of optimization passes is called a \textit{policy}, which together performs a specific goal.
For example, in order to optimize an application to run on evictable transient resources, we can use a specialized executor placement pass, that places each computations appropriately on different types of resources, and data flow model pass, that determines the fashion in which each computation should fetch its input data, with a number of other passes for further optimization.
This enables users to flexibly customize and perform data processing for different \textit{deployment characteristics} by simply plugging in different policies for specific goals.
This greatly simplifies the work by replacing the work of exploring and rewriting system internals for modifying runtime behaviors with a simple process of using pluggable policies.
It also makes it possible for the system to promptly meet new requirements through easy extension of system capabilities.
