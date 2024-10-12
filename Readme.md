# DNANexus

## Read random line from file

This program implements a solution for retrieving a random line from a file based on the following assumptions:

1. Line endings are \n
2. Assumes encoding is UTF-8
3. Max characters per line is 1000
4. Tries to strike a balance between performance, memory and storage usage

### Implementation

We read the input file line-by-line so we don't have to allocate xGB of memory.

Create a sparse index based on chunk size, in our case every 1000 lines to reduce the size of the index file.
Creating the index is done in parallel using go-routines and channels.

Builds an index mapping line numbers to file offsets, allowing us to seek directly to the vicinity of the desired line and read only a small portion of the file.

Then we search for the line in the chunk specified in the index.
Each entry in the index file contains a line number and the corresponding byte offset.
The index records every 1000th line (configurable via IndexInterval), balancing between index size and lookup speed.

Although this slightly increases the lookup time, the performance hit is minimal and worth the
trade-off so we don't end up with a xGB index file, and we can load the index file into memory since it's much smaller
so if this is code would accept e.g. http requests the index file can be cached into memory, and we don't have to read it
each time we want to retrieve a random line.

## How To

Install golang on you system, you can download a specific version [here](https://go.dev/dl/).
This code is set up to use version 1.19.
Once golang is installed on you system, open a terminal and cd into the directory containing the main.go file.

### Run

```shell
go run main.go ./input.txt 666
```
