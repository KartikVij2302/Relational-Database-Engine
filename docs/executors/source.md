# Source Functionality Documentation

## Overview
The `source.cpp` file implements functionality for handling the `SOURCE` command in a query language. It consists of three main functions:

1. **Syntactic Parsing**: Validates the command format to ensure it follows the syntax `SOURCE filename`. If the syntax is incorrect, it returns an error.

2. **Semantic Parsing**: Checks if the specified source file exists. If the file is not found, it logs an error.

3. **Execution**: Reads commands from the specified source file and executes them. The commands are processed line by line, and any commands found are executed using the `doCommand()` function.

## Outcome
The successful execution of the `SOURCE` command allows for dynamic command processing based on the contents of the specified file, enabling flexible query handling.
