
# Relational-Database

A minimalist, integer-only, read-optimized Relational Database Management System built from scratch in C++ for the Data Systems course. The project demonstrates core database internals, including relational algebra, external sorting, indexing, and basic query optimization.

## Key Characteristics

* **Integer-only data types** for simplified, educational clarity
* **Read-optimized design** focused on efficient query processing
* **Block-based storage** simulating realistic disk I/O
* **FIFO buffer management** with configurable pool size
* **Single-threaded execution** for a simplified concurrency model

## Core Functionality

* Relational operators: **SELECT, PROJECT, JOIN, CROSS**
* **External sorting** via K-way merge sort
* **Hash-based join** mechanisms
* **Indexing structures** for optimized lookup
* **Matrix operations:** load, transpose, rotation
* **GROUP BY** with MAX, MIN, COUNT, SUM, AVG
* **ORDER BY** with multi-column sorting
* **INSERT, UPDATE, DELETE** for data manipulation
* **SOURCE** command for batch query execution

## Supported Operations

| Category          | Operations                                        |
| ----------------- | ------------------------------------------------- |
| Data Definition   | LOAD, CLEAR, LIST TABLES                          |
| Data Manipulation | INSERT, UPDATE, DELETE                            |
| Query Operations  | SELECT, PROJECT, JOIN, SEARCH                     |
| Aggregation       | GROUP BY, ORDER BY, SORT                          |
| Matrix Operations | LOAD MATRIX, ROTATE, CROSSTRANSPOSE, CHECKANTISYM |
| System Commands   | EXPORT, RENAME, SOURCE, QUIT                      |

---

Let me know if you'd like a shorter version, a more narrative style, or GitHub-friendly formatting!
