# SDF Tools

A collection of CLI tools for editing and filtering docking results made by CmDock or similar software.
The SDF Tools Suite now supports unlimited file sizes. Support for CmZIP archives is planned, but has yet to be implemented.

## Build

### Prerequisites

* [Rust](https://www.rust-lang.org/)

To build every tool run:

```
cargo build --release
```

## Manual

### sdcount

Returns number of SDF records in file or stdin.

```
USAGE:
    sdcount <INPUT>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    -z, --zip        Input file is a zip container (currently only
                     supports zips with a single file)

ARGS:
    <INPUT>    Sets input file. If missing
               reads from stdin.
```

### sddistill

Distills values of a specified data field in SD records.

```
USAGE:
    sddistill [FLAGS] [OPTIONS] --input <INPUT> --output <OUTPUT>

FLAGS:
    -h, --help         Prints help information
    -r, --recursive    Enables recursive directory crawling
    -V, --version      Prints version information
    -z, --zip          Input file is a zip container (currently only
                       supports zips with a single file)

OPTIONS:
    -i, --input <INPUT>           Sets input. Can be file or directory.
                                  Reads from stdin if set to -
    -o, --output <OUTPUT>         Sets output directory. Must not be
                                  . as the output filename is the
                                  same as the input filename.
    -p, --pattern <PATTERN>       SDF field to write out. [default: SCORE]
```

### sdfilter

Outputs file(s) with records that fits specified criterium.

```
USAGE:
    sdfilter [FLAGS] --field <FIELD> --input <INPUT> --operand <operand>
    --output <OUTPUT> --value <VALUE>

FLAGS:
    -c, --concat     Gathers results in a single file.
    -h, --help       Prints help information
    -V, --version    Prints version information
    -z, --zip        Input file is a zip container (currently only supports
                     zips with a single file)

OPTIONS:
    -i, --input <INPUT>        Sets input. Can be file or directory.
                               - reads from stdin
    -o, --output <OUTPUT>      Sets output directory. Must not be
                               . as the output filename is the
                               same as the input filename.
    -f, --field <FIELD>        SDF field to check. [default: SCORE]
    -O, --operand <operand>     [values: lt, le, eq, ne, ge, gt]
    -v, --value <VALUE>        Value to be compared to.
```

### sdreport

Produces text summaries of SD records.

```
USAGE:
    sdreport [FLAGS] [OPTIONS]

FLAGS:
    -l, --list       List format: output all data fields for each record.
    -t, --table      Table format: tabulate selected fields for each record 
                     as processed.
    -c, --csv        CSV format: comma delimited output of selected fields
                     for each record as processed.
    -s, --summary    Summary format: output summary statistics for each
                     unique value of ligand ID.
    -n, --norm       Use normalised score field names as default columns in
                     -t and -c formats (normalised = score / #ligand heavy atoms).
    -o, --old        Use old (v3.00) score field names as default columns
                     in -t and -c formats, else use v4.00 field names.
        --no-head    Don't output column headings in -t and -c formats.
    -h, --help       Prints help information
    -V, --version    Prints version information
    -z, --zip        Input file is a zip container (currently only supports
                     zips with a single file)

OPTIONS:
    -i, --input <input>        Sets input file. If missing or - reads from
                               stdin. [default: -]
    -I, --idfield <idfield>    Data field to use as ligand ID. [default:
                               _TITLE1]
    -f, --fields <fields>      Fields to include in report (idfield is 
                               always included)
```

### sdseparate

Separates file by id field

```
USAGE:
    sdseparate [FLAGS] [OPTIONS] --input <INPUT>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    -z, --zip        Input file is a zip container (currently only supports
                     zips with a single file)

OPTIONS:
    -i, --input <INPUT>      Sets input file. - reads from stdin.
    -o, --output <OUTPUT>    Sets output directory. [default: . ]
    -f, --field <FIELD>      Specifies id field. [default: _TITLE1]
```

### sdsort

Sorts SD records by given data field.

```
USAGE:
    sdsort [FLAGS] [OPTIONS] --input <INPUT>

FLAGS:
    -n, --num        Numeric sort (default is text sort).
    -r, --reverse    Descending sort (default is ascending sort).
    -z, --zip        Input file is a zip container (currently only supports
                     zips with a single file).
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -i, --input <INPUT>         Sets input file. - reads from stdin.
    -o, --output <OUTPUT>       Sets output directory. - writes to stdout.
                                [default: -]

    -f, --field <SORT FIELD>    Specifies sort field. [default: SCORE]
    -g, --group <ID FIELD>      Sort results by group. Group by
                                specified data field. [default: _TITLE1]
```

### sdsplit

Splits SDF file into a specified number of equal files or files with an equal number of records.

```
USAGE:
    sdsplit [OPTIONS] --input <INPUT> --output <OUTPUT> 
    [--files <NUM OF PACKAGES>/--size <PACKAGE SIZE>]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    -z, --zip        Input file is a zip container (currently only
                     supports zips with a single file)

OPTIONS:
    -i, --input <INPUT>              Sets input file. - reads from stdin.
    -f, --files <NUM OF PACKAGES>    Sets nr of packeges to split input into.
    -s, --size <PACKAGE SIZE>        Sets size of packages to split input into.
    -o, --output <OUTPUT>            Sets output directory. [default: .]
    -n, --name <NAME>                Sets output file names. This is
                                     always suffixed with _# [default: temp]
    
    --files and --size are incompatible, but at least one must be specified
```

### lineconv

Converts between CRLF and LF line separators. Replaces any non-UTF8 data with �.

```
USAGE:
    lineconv [FLAGS] [OPTIONS] --crlf --lf

FLAGS:
        --lf                 Sets EoL sequence to LF (unix)
        --crlf               Sets EoL sequence to CRLF (DOS)
    -n, --non-destructive    Keeps non-UTF8 data untouched
    -h, --help               Prints help information
    -V, --version            Prints version information
    -z, --zip                Input file is a zip container
                             (currently only supports zips
                             with a single file)

OPTIONS:
    -i, --input <INPUT>      Sets input file. If missing
                             reads from stdin.
    -o, --output <OUTPUT>    Sets output file. If missing
                             writes to stdout.
    
```