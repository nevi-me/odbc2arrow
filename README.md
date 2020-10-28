# ODBC to Apache Arrow

This is a hopefully lean library that allows you to consume ODBC records as 
Arrow `RecordBatch`es. It does not write these batches to files or streams,
as that is simple enough for the implementer to do.

## Origins

We based this library on the excellent work from Markus Klein's [odbc2parquet](https://github.com/pacman82/odbc2parquet) crate.

However, instead of making this a binary, we made it a library.
Apache Arrow is an in-memory format, so it makes more sense to read data into memory, rather than being opinionated about how to write files.

## Usage

TODO