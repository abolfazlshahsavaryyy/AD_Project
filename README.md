# Task Allocation and Scheduling in Distributed Systems

Project: Task Allocation and Scheduling in Distributed Systems using Extended Minimum Cost Maximum Flow and Dynamic Programming
Author: Sayin Ala

## Introduction

Efficient task allocation and scheduling are critical in modern distributed computing systems. As distributed systems scale, traditional allocation methods fail to handle resource heterogeneity, execution costs, task dependencies, and deadlines effectively.

This project presents a multi-phase framework combining Minimum Cost Maximum Flow (MCMF) for global task allocation and Dynamic Programming (DP) for local scheduling. The framework is designed to:

Minimize total execution and communication costs.

Respect node resource constraints.

Handle task dependencies and deadlines.

Adapt dynamically to runtime events such as node failures or new task arrivals.

## Key Features

Multi-Phase Architecture: From static allocation to dynamic runtime adjustments.

Graph-Based Task Allocation (MCMF): Optimizes task-to-node assignments under cost and resource constraints.

Time-Aware Scheduling: Handles task deadlines and time windows.

Dependency Support: Ensures tasks with precedence constraints are scheduled correctly.

Dynamic Reconfiguration: Responds to runtime events like node failures or task injections.

Local Scheduling via DP: Optimizes task execution order on each node.

Customizable Test Framework: Allows generation of tasks, nodes, and scenarios.

Modular & Extensible: Supports plugin strategies, variable cost models, and integration with real-world simulators.

Performance-Oriented: Scales efficiently for hundreds of tasks and nodes.


# Architecture and Phases
## Phase 1: Initial Task-to-Node Allocation

Goal: Assign tasks to compute nodes optimally using MCMF.

Problem Modeling: Represent tasks as demand nodes, nodes as supply nodes, and assignments as edges.

Constraints: Resource capacity, one-to-one assignment, execution cost minimization.

Flow Graph Structure:

Source → Task: capacity 1, cost 0

Task → Node: capacity 1, cost = execution cost

Node → Sink: capacity = node task limit, cost 0

Output: Task-to-node assignments and total execution cost.


## Phase 2: Time-Aware Scheduling and Dependency Handling

Goal: Respect deadlines and task dependencies.

Methods:

Time-Expanded Flow Network: Tasks replicated across time slots.

Two-Stage Model: Phase 1 MCMF for allocation, then DP/greedy for scheduling.

Constraints: Deadlines, earliest start times, task dependencies, node resource availability per time slot.

Output: Valid schedule for each task with start times and deadline adherence.


## Phase 3: Dynamic Reallocation

Goal: Adapt to runtime events without full recomputation.

Events Handled: Node failures, new task arrivals, resource changes, delays.

Strategy: Incremental flow updates, partial rescheduling, cost-aware reassignment.

Output: Updated task schedule, reassigned and failed tasks, total cost, change penalty.


## Phase 4: Local Task Scheduling using Dynamic Programming

Goal: Optimize task execution order on individual nodes.

Input: Assigned tasks per node, CPU/RAM per time slot, task durations and deadlines.

DP Model: Maximize tasks executed or minimize penalties while respecting resource constraints.

Output: Per-task execution schedule, deadline adherence, idle time, and penalty cost.
