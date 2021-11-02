#![allow(non_snake_case)]

use std::{
    fs::metadata,
    io::BufReader,
    time::Instant
};
use clap::{load_yaml, App};
use indicatif::{HumanDuration, ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::prelude::*;

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/sddistill.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let filetypes: Vec<&str> = vec!["test"]; //matches.values_of("filetypes").unwrap().collect();
    let _zipped = matches.is_present("zipped");

    // Iterate over input files 
    let files: Vec<String>;
    if let Some(path) = matches.value_of("input") {
        if path == "-"{ // If input is "-" then read from stdin
            files = vec![(&path).to_string()];
        } else if metadata(&path).unwrap().is_dir() { // Check if path points to dir
            files = sdf::getFiles(&path, filetypes.clone(), (&matches.is_present("recursive")).to_owned());
        } else { // Check if path points to a file
            files = vec![(&path).to_string()];
        }

        // Draw a nice progress bar
        let started = Instant::now();
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({eta} @ {per_sec})")
            .progress_chars("#>-"));

        // Iterate over files in directory (or single specified file)
        println!("Processing files...");
        let _iter: Vec<_> = files.par_iter().progress_with(pb).map(|file| { // Use par_iter() for easy parallelization
            // Read file contents to string
            let mut reader = BufReader::new(std::fs::File::open(&file).unwrap());
            let mut output: Vec<String> = Vec::new();
            loop {
                let block = match sdf::record_to_string(&mut reader) {
                    Some(block) => block,
                    None => break
                };

                // Get specified field from record and store value in vector
                output.push(extractData(&block, matches.value_of("pattern").unwrap()).to_string());
            }
            // Set output path
            let out_path: String = match file.trim() {
                "-" => (matches.value_of("output").unwrap().to_owned() + "/stdin.txt"),
                _ => (matches.value_of("output").unwrap().to_owned() + "/" + (&file.split("/").collect::<Vec<&str>>()).last().unwrap()),
            };
            // Write vector of extracted data to file
            sdf::write_to_file(&(output.join("\n")), &out_path);
        }).collect();
        // pb.finish();

        // Print time elapsed
        println!("Done in {}", HumanDuration(started.elapsed()));
    } else {
        // Exit if input doesn't exist
        std::process::exit(0x0100);
    }

}

fn extractData(sdfblock: &str, pattern: &str) -> String {
    let firstSplit: Vec<&str> = sdfblock.split(pattern).collect();
    if firstSplit.len() < 2 {
        println!("Data field not found in block\n{}\n", &sdfblock);
        return "N/A".to_owned();
    } else {
        return firstSplit[1].split(">").collect::<Vec<&str>>()[1].trim().to_string();
    }
}