extern crate clap;
use clap::{load_yaml, App};
use std::{
    io::{self, prelude::*, BufReader},
};

// Returns number of SDF records in file or stdin

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdcount.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let input = match matches.value_of("input"){
        Some(input) => input,
        None => "-"
    };

    let mut reader: Box<dyn BufRead> = match input {
        "-" => Box::new(BufReader::new(io::stdin())),
        _ => Box::new(BufReader::new(std::fs::File::open(input).unwrap()))
    };
    
    let count = sdf::count_records(&mut reader);

    // Print number of SDRecords in string
    println!("{}", count.to_string())

}