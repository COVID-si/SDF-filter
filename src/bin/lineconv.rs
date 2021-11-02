use std::{
    path::Path,
    io::{self, Write, BufReader, BufRead}
};
use fs_err::File;
use clap::{load_yaml, App};

// Converts between CRLF and LF line separators

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/lineconv.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let input = match matches.value_of("input").unwrap() {
        "-" => {sdf::Input::Stdin(io::stdin())},
        filename => {sdf::Input::File(File::open(Path::new(filename)).expect("No such file!"))}
    };
    let output = matches.value_of("output").unwrap();

    // TODO: implement reading and writing to same file
    if output == input.filename() {
        eprintln!("Cannot write to source file, specify other output or write to stdout!");
        std::process::exit(0x0100);
    }

    // Read file lines as vector of bytes (by line), don't destroy non UTF8 data
    let mut reader = BufReader::new(input);
    let mut writer: Box<dyn Write> = match output {
        "-" => Box::new(io::stdout()),
        filename => Box::new(fs_err::File::create(Path::new(filename)).expect("Cannot create file!"))
    };

    let mut buf: Vec<u8> = Vec::new();
    while let Ok(_) = reader.read_until(b'\n', &mut buf) {
        if buf.is_empty() {
            break;
        }
        // Remove LF
        &buf.pop();
        // Remove CR if present
        if buf.last() == Some(&b'\r') {
            &buf.pop();
        }

        // Check if file should be read destructively
        if !matches.is_present("bytes") {
            buf = String::from_utf8_lossy(&buf).as_bytes().to_vec();
        }

        // Add CRLF or LF as bytes to the end of each line
        match matches.is_present("crlf") {
            true => {
                &buf.push(b'\r');
                &buf.push(b'\n')
            },
            false => &buf.push(b'\n')
        };

        // Write to file or stdout
        writer.write(&buf).expect("Cannot write to output!");
        buf.clear();
    }
}