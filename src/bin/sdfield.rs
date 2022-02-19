use std::{
    io::{self, prelude::*, BufWriter, BufReader},
    fs::metadata,
    time::Instant
};
use clap::{load_yaml, App};
use indicatif::{HumanDuration, ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::prelude::*;

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdfield.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let input = matches.value_of("input");
    let output = matches.value_of("output").unwrap();
    let field = matches.value_of("field").unwrap();
    let value = match matches.value_of("value") {
        Some(val) => val,
        None => ""
    };

    let addition: String = String::from(">  <") + field + ">\n" + value + "\n\n";
    
    match input {
        Some(path) => {
            // Iterate over input files 
            let files: Vec<String>;
            if path == "-"{ // If input is "-" then read from stdin
                files = vec![(&path).to_string()];
            } else if metadata(&path).unwrap().is_dir() { // Check if path points to dir
                files = sdf::getFiles(&path, vec![], (&matches.is_present("recursive")).to_owned());
            } else { // Check if path points to a file
                files = vec![(&path).to_string()];
            }

            if output == "-" {
                // Create write buffer
                let mut writer = Box::new(BufWriter::new(io::stdout()));
                for file in files {
                    let mut reader = BufReader::new(std::fs::File::open(&file).unwrap());
                    append(&mut reader, &mut writer, &addition);
                }
            } else {
                // Draw a nice progress bar
                let started = Instant::now();
                let pb = ProgressBar::new(files.len() as u64);
                pb.set_style(ProgressStyle::default_bar()
                    .template("{spinner} [{elapsed_precise}] [{wide_bar}] {pos}/{len} ({eta} @ {per_sec})")
                    .progress_chars("#>-"));
                
                // Iterate over files in directory (or single specified file)
                println!("Processing files...");
    
                let _iter: Vec<_> = files.par_iter().progress_with(pb).map(|file| { // Use par_iter() for easy parallelization
                    // Set output path
                    let out_path: String = match file.trim() {
                        "-" => (output.to_owned() + "/stdin.txt"),
                        _ => (output.to_owned() + "/" + (&file.split("/").collect::<Vec<&str>>()).last().unwrap()),
                    };
    
                    // Create write buffer
                    let out_file = sdf::create_file(&out_path);
                    let mut writer = Box::new(BufWriter::new(out_file));
                    let mut reader = BufReader::new(std::fs::File::open(&file).unwrap());
                    
                    append(&mut reader, &mut writer, &addition);

                }).collect();
    
                println!("Done in {}", HumanDuration(started.elapsed()));
            }
        },
        None => {}
    }

}

fn append<R: BufRead, W: Write>(reader: &mut R, writer: &mut W, value: &String) {
    loop {
        let mut buf: Vec<u8> = Vec::new();
        match reader.read_until(b'\n', &mut buf) {
            Ok(_) => {
                if buf.is_empty() {
                    break;
                }
                &buf.pop();
                if buf.last() == Some(&b'\r') {
                    &buf.pop();
                }
                let line = String::from_utf8_lossy(&buf);
                if line.contains("$$$$") {
                    writer.write_all(value.as_bytes()).unwrap();
                }
                buf.push(b'\n');
                writer.write_all(&buf).unwrap();
                buf.clear();
            }
            Err(_) => {}
        };
    }
}