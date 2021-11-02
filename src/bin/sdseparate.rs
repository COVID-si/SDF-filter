use clap::{load_yaml, App};
use sdf::{
    sdfrecord::SDFRecord,
    Input
};
use std::{
    io::{prelude::*, BufReader, BufWriter},
};

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdseparate.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let input: Input = match matches.value_of("input").unwrap() {
        "-" => Input::Stdin(std::io::stdin()),
        filename => Input::File(fs_err::File::open(std::path::Path::new(filename)).expect("No such file"))
    };
    let inputfilename = &input.filename();
    let output = matches.value_of("output").unwrap();
    let idfield = matches.value_of("idfield").unwrap();

    let mut reader = BufReader::new(input);
    let mut writer: BufWriter<fs_err::File>;
    
    let mut i = 1;
    let mut previous = "".to_string();

    loop {

        let block = match sdf::record_to_lines(&mut reader) {
            Some(block) => block,
            None => break
        }; 

        let mut record: SDFRecord = SDFRecord::new();
        record.readRec(block);
        if record.getData("_NATOMS") == "ERR" {
            eprintln!("Invalid count line in {}[{}]", inputfilename, i.to_string());
            continue;
        }

        let current = record.getData(idfield);
        
        let filename = output.to_owned() + "/" + &current + ".sdf";
        let file: fs_err::File;
        if previous == "".to_string() || previous != current {
            previous = current.clone();
            file = sdf::create_file(&filename);
        } else {
            file = fs_err::OpenOptions::new().append(true).open(&filename).expect("Error writing to file!");
        }

        writer = BufWriter::new(file);
        writeln!(writer, "{}\n$$$$", record.lines.join("\n")).expect("Error writing to buffer!");
        writer.flush().expect("Error flushing buffer!");

        i = i + 1;
    }
}