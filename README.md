# Splitting input file plugin for Embulk

This Embulk plugin splits and inputs a text file.
By splitting a file, input tasks will be executed in multithreads and the performance will be improved.

Lines of the text file should be separated by CR or LF or CRLF.
The plugin searches line separators and splits a file properly.

## Overview

* **Plugin type**: input

## Configuration

- **path**: the path of a text file (string, either this or path_prefix is required)
- **path_prefix**: the path prefix of text files (string, either this or path_prefix is required)
- **header_line**: whether the first line is a header or not (boolean, default: false)
- **tasks**: number of tasks (integer, default: number of available processors * 2)

### Example

```yaml
in:
  type: filesplit
  path: '/data/address.csv'
  header_line: true
  tasks: 4
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    header_line: true
    delimiter: ','
    ...
```

### Build

```
$ ./gradle gem
```
