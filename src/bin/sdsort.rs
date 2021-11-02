use clap::{load_yaml, App};
use sdf::sdfrecord::SDFRecord;
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use rayon::prelude::*;

fn main() {
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdsort.yml");
    let matches = App::from_yaml(yaml).get_matches();

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
                    lines.extend(record.lines.clone());
                    lines.push("$$$$".to_string());
                }
            }
        }
        sdf::write_to_file(&(lines.join("\n")), &out_path);
    }
}