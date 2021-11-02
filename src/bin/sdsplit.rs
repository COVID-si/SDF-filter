extern crate clap;
use clap::{load_yaml, App};
use std::{
    io::{self, prelude::*, BufReader, BufWriter},
};

fn main(){
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdsplit.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let input = matches.value_of("input").unwrap();
    let output = matches.value_of("output").unwrap();
    let name = matches.value_of("name").unwrap();
    let zipped = matches.is_present("zipped");

    // Check if package should be split by number of files or number of records
    match matches.is_present("number_of_files") {
        true => split_package_num(&input, matches.value_of("number_of_files").unwrap().parse::<usize>().unwrap(), output, name, zipped),
        false => split_package_size(&input, matches.value_of("size_of_files").unwrap().parse::<usize>().unwrap(), output, name, zipped)
    }
}

fn split_package_num(package: &str, threads: usize, outputdir: &str, prefix: &str, _zipped: bool) {

    // Reading from stdin is not supported here
    // ... for now
    if package == "-" {
        println!("Splitting stdin is only supported by size of files, not number of files!");
        std::process::exit(0x0100);
    }

    // Get number of records in file
    let mut reader = BufReader::new(std::fs::File::open(package).unwrap());
    let num_rec = sdf::count_records(&mut reader);

    // Calculate number of SDRecords per file
    let div: usize = num_rec / threads;
    let rem: usize = num_rec % threads;

    // Store number of SDRecords in vector of ints
    let mut temp: Vec<usize> = Vec::new();
    let mut i = 1;
    while i <= threads {
        if i <= rem {
            temp.push(div + 1);
        } else {
            temp.push(div);
        }
        i = i + 1;
    }

    // Iterate over vector of SDRecords per file
    let mut reader = BufReader::new(std::fs::File::open(package).unwrap());
    let mut buf: Vec<u8> = Vec::new();
    let mut current = 1;
    for size in &temp {
        // Create output file
        let filename = outputdir.to_owned() + "/" + &prefix + "_" + &(current).to_string() + ".sdf";
        let file = sdf::create_file(&filename);

        // Create a writer to speed up the output
        let mut writer = BufWriter::new(file);

        let mut n = 0;
        // Store SDRecords in vectors of size itm
        while &n < size {
            // Iterate over lines in record
            'sub: loop {
                match reader.read_until(b'\n', &mut buf) {
                    Ok(_) => {
                        // Read line from buffer
                        &buf.pop();
                        if buf.last().unwrap() == &b'\r' {
                            &buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        
                        // Add line to writer
                        writeln!(writer, "{}", line).expect("Failed to write to buffer!");
                        
                        // Flush writer to file
                        if line.contains("$$$$") { 
                            buf.clear();
                            writer.flush().unwrap();
                            break 'sub;
                        }

                        // Empty buffer
                        buf.clear();
                    }
                    Err(e) => eprintln!("{}", e)
                };
            }
            n = n + 1;
        }
        current = current + 1;
    }
}

fn split_package_size(package: &str, size: usize, outputdir: &str, prefix: &str, _zipped: bool) {

    let mut reader: Box<dyn BufRead> = match package {
        "-" => Box::new(BufReader::new(io::stdin())),
        _ => Box::new(BufReader::new(std::fs::File::open(package).unwrap()))
    };
    let mut buf: Vec<u8> = Vec::new();

    let mut current = 1;
    'main: loop {
        // Create output file
        let filename = outputdir.to_owned() + "/" + &prefix + "_" + &(current).to_string() + ".sdf";
        let file = sdf::create_file(&filename);

        // Create a writer to speed up the output
        let mut writer = BufWriter::new(file);
        
        // Iterate over records in file
        // Loop breaks after the specified number of records has been read
        for _i in 0..size {
            // Iterate over lines in record
            'sub: loop {
                match reader.read_until(b'\n', &mut buf) {
                    Ok(_) => {
                        // Read line from buffer
                        if buf.is_empty() {
                            break 'main;
                        }
                        &buf.pop();
                        if buf.last().unwrap() == &b'\r' {
                            &buf.pop();
                        }
                        let line = String::from_utf8_lossy(&buf);
                        
                        // Add line to writer
                        writeln!(writer, "{}", line).expect("Failed to write to buffer!");
                        
                        // Flush writer to file
                        if line.contains("$$$$") { 
                            buf.clear();
                            writer.flush().unwrap();
                            break 'sub;
                        }

                        // Empty buffer
                        buf.clear();
                    }
                    Err(e) => eprintln!("{}", e)
                };
            }
        }
        current = current + 1;
    }
}