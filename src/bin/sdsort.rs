use clap::{load_yaml, App};
use sdf::sdfrecord::SDFRecord;
use ordered_float::OrderedFloat;
use std::{
    collections::HashMap,
    fs::File,
    io::{prelude::*, Read, Seek, SeekFrom, BufReader, BufWriter}
};
use rayon::prelude::*;

struct Entry {
    value: String,
    value_num: Option<f64>,
    group: Option<String>, 
    index: u64,
    size: u64
}

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdsort.yml");
    let matches = App::from_yaml(yaml).get_matches();

    match matches.is_present("large-files") {
        true => large_file_sort(matches),
        false => sort(matches)
    }
}

fn large_file_sort(matches: clap::ArgMatches) {
    
    let input = matches.value_of("input").unwrap();
    if input == "-" {
        eprintln!("Stdin cannot be used with large file support!");
        std::process::exit(0x0100);
    }
    let output = matches.value_of("output").unwrap();
    let sort_field = matches.value_of("sort_field").unwrap();
    let group = match matches.is_present("group") {
        true => matches.value_of("group"),
        false => None
    };
    let num_sort = matches.is_present("numeric_sort");

    let mut grouped_records: HashMap<Option<String>, Vec<Entry>> = HashMap::new();

    let mut reader: BufReader<File> = BufReader::new(File::open(input).unwrap());
    let mut position: u64;
    
    let mut i = 0;
    loop { 
        position = reader.seek(SeekFrom::Current(0)).unwrap();
        let block = match sdf::record_to_lines(&mut reader) {
            Some(block) => block,
            None => break
        };
        let size = reader.seek(SeekFrom::Current(0)).unwrap() - position;

        let mut record = sdf::sdfrecord::SDFRecord::new();
        record.readRec(block);
        if record.getData("_NATOMS") == "ERR" {
            eprintln!("Invalid count line in {}[{}]", input, i.to_string());
            continue;
        }
        i = i + 1;
        let data = record.getData(&sort_field);

        let group_id = match group {
            Some(id) => Some(record.getData(id)),
            None => None
        };

        if !grouped_records.contains_key(&group_id) {
            grouped_records.insert(group_id.clone(), Vec::new());
        }
        grouped_records.get_mut(&group_id).unwrap().push(
            Entry {
                value_num: match num_sort {
                    true => Some(data.parse::<f64>().unwrap()),
                    false => None
                },
                value: data,
                group: group_id,
                index: position,
                size: size
            }
        );

    }

    let mut writer = match output {
        "-" => {
            Box::new(BufWriter::new(std::io::stdout())) as Box<dyn Write>
        },
        path => {
            let out_file = sdf::create_file(&path);
            Box::new(BufWriter::new(out_file)) as Box<dyn Write>
        }
    };

    for (_id, mut records) in grouped_records {
        if num_sort {
            records.par_sort_unstable_by(|a, b| a.value_num.unwrap().partial_cmp(&b.value_num.unwrap()).unwrap());
        } else {
            records.par_sort_unstable_by(|a, b| a.value.partial_cmp(&b.value).unwrap());
        }

        for entry in records {
            let mut bytes = vec![0u8; entry.size as usize];
            reader.seek(SeekFrom::Start(entry.index)).unwrap();
            reader.read_exact(&mut bytes).unwrap();
            writer.write_all(&mut bytes).unwrap();
        }
    }
}

fn sort(matches: clap::ArgMatches) {

    let input = matches.value_of("input").unwrap();
    let output = matches.value_of("output").unwrap();
    let sort_field = matches.value_of("sort_field").unwrap();
    let idfield = matches.value_of("group").unwrap();
    
    let mut records: Vec<SDFRecord> = sdf::file_to_SDF_vec(input);

    let mut grouped_records: HashMap<String, Vec<SDFRecord>> = HashMap::new();
    if matches.is_present("group") {
        for record in records {
            let id = record.getData(idfield);
            if grouped_records.contains_key(&id) {
                grouped_records.get_mut(&id).unwrap().push(record);
            } else {
                grouped_records.insert(id, vec![record]);
            }
        }
        records = Vec::new();
        for (_id, mut recordss) in grouped_records {
            if matches.is_present("numeric_sort") {
                recordss.par_sort_unstable_by_key(|record| OrderedFloat(record.getData(&sort_field).parse::<f64>().unwrap()));
                records.extend(recordss);
            } else {
                recordss.par_sort_unstable_by_key(|record| record.getData(&sort_field));
                records.extend(recordss);
            }
        }
    } else { 
        if matches.is_present("numeric_sort") {
            records.par_sort_unstable_by_key(|record| OrderedFloat(record.getData(&sort_field).parse::<f64>().unwrap()));
        } else {
            records.par_sort_unstable_by_key(|record| record.getData(&sort_field));
        }
    }

    if output == "-" {
        match matches.is_present("reverse") {
            true => {
                for record in records.iter().rev() {
                    record.writeRec();
                }
            },
            false => {
                for record in records.iter() {
                    record.writeRec();
                }
            }
        }
    } else {
        // Set output path
        let out_path: String = match input.trim() {
            "-" => (matches.value_of("output").unwrap().to_owned() + "/stdin.txt"),
            _ => (matches.value_of("output").unwrap().to_owned()),
        };
        let mut lines: Vec<String> = Vec::new();
        match matches.is_present("reverse") {
            true => {
                for record in records.iter().rev() {
                    lines.extend(record.lines.clone());
                    lines.push("$$$$".to_string());
                }
            },
            false => {
                for record in records.iter() {
                    println!("{}", record.getData(idfield));
                    lines.extend(record.lines.clone());
                    lines.push("$$$$".to_string());
                }
            }
        }
        sdf::write_to_file(&(lines.join("\n")), &out_path);
    }
}