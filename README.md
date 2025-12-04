
# Relational-Database

A minimalist, integer-only, read-focused Relational Database Management System built entirely in C++. The project showcases essential database internals such as relational algebra, external sorting, indexing, and foundational query optimization techniques.

## Key Characteristics

* **Integer-only schema** to keep the type system simple and instructional
* **Read-optimized architecture** prioritizing fast query performance
* **Block-based storage engine** that emulates real-world disk I/O
* **FIFO-based buffer pool** with adjustable buffer size
* **Single-threaded execution model** for simplified concurrency handling

## Core Functionality

* Relational operators: **SELECT, PROJECT, JOIN, CROSS**
* **External K-way merge sort** for scalable sorting
* **Hash join** strategies for efficient table joins
* **Indexing mechanisms** to accelerate query execution
* **Matrix utilities:** load, transpose, rotate
* **GROUP BY** with aggregates: MAX, MIN, COUNT, SUM, AVG
* **ORDER BY** with multi-column comparison
* **INSERT, UPDATE, DELETE** for modifying data
* **SOURCE** command for executing batched queries

## Supported Operations

| Category          | Operations                                        |
| ----------------- | ------------------------------------------------- |
| Data Definition   | LOAD, CLEAR, LIST TABLES                          |
| Data Manipulation | INSERT, UPDATE, DELETE                            |
| Query Operations  | SELECT, PROJECT, JOIN, SEARCH                     |
| Aggregation       | GROUP BY, ORDER BY, SORT                          |
| Matrix Operations | LOAD MATRIX, ROTATE, CROSSTRANSPOSE, CHECKANTISYM |
| System Commands   | EXPORT, RENAME, SOURCE, QUIT                      |



