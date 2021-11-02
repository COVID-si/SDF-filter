use std::{
    collections::BTreeMap,
    io::{self, Write}};
use clap::{load_yaml, App};
use prettytable::{format, Table, Row, Cell};
use sdf::sdfrecord::SDFRecord;
use ordered_float::OrderedFloat;

fn main() {
    
    // Collect help information and arguments
    let yaml = load_yaml!("help/sdreport.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let input = matches.value_of("input").expect("No input value");
    let contents = sdf::file_to_SDF_vec(input);
    let idfield = String::from(matches.value_of("idfield").unwrap());

    if !matches.is_present("table") && !matches.is_present("csv") && !matches.is_present("summary") {
        output_list(contents);
        std::process::exit(0x0100);
    }

    let headings: Vec<&str>;
    let fields: Vec<&str>;
    if matches.is_present("fields") {
        // Field names passed from user
        headings = matches.values_of("fields").unwrap().collect();
        fields = headings.clone();
    } else if matches.is_present("norm") {
        // Default field names and headings for normalized scores (score / #ligand heavy atoms)
        headings = vec!["TOTALn", "INTERn", "INTRAn", "RESTRn", "#heavy"];
        fields = vec!["SCORE.norm", "SCORE.INTER.norm", "SCORE.INTRA.norm", "SCORE.RESTR.norm", "SCORE.heavy"];
    } else if matches.is_present("old") {
        // Default field names and headings for rDock v3.00 scores
        headings = vec!["TOTAL", "INTER", "INTRA", "INTRAMIN", "RESTR"];
        fields = vec!["Rbt.Score.Corrected", "Rbt.Score.Inter", "Rbt.Score.Intra", "Rbt.Score.IntraMin", "Rbt.Score.Restraint"];
    } else {
        // Default field names and headings for rDock v4.00 and newer (incl. RxDock & CurieDock) scores
        headings = vec!["TOTAL", "INTER", "INTRA", "RESTR", "VDW"];
        fields = vec!["SCORE", "SCORE.INTER", "SCORE.INTRA", "SCORE.RESTR", "SCORE.INTER.VDW"];
    }

    if matches.is_present("summary") {
        if matches.is_present("csv") && matches.is_present("table") {
            output_summary(contents, &idfield, "csv,table", !matches.is_present("no_headers"), headings, fields);
            std::process::exit(0x0100);
        } else if matches.is_present("csv") {
            output_summary(contents, &idfield, "csv", !matches.is_present("no_headers"), headings, fields);
            std::process::exit(0x0100);
        } else if matches.is_present("table") {
            output_summary(contents, &idfield, "table", !matches.is_present("no_headers"), headings, fields);
            std::process::exit(0x0100);
        } else {
            output_summary(contents, &idfield, "list", !matches.is_present("no_headers"), headings, fields);
            std::process::exit(0x0100);
        }
    }

    if matches.is_present("csv") || matches.is_present("table") {
        let mut contents_data: Vec<BTreeMap<String, Vec<String>>> = Vec::new();
        for record in contents {
            contents_data.push(record.data);
        }
        let table = make_table(contents_data, !matches.is_present("no_headers"), &idfield, headings, fields);
        if !matches.is_present("table") {
            output_csv(table);
            std::process::exit(0x0100);
        } else if !matches.is_present("csv") {
            output_table(table);
            std::process::exit(0x0100);
        }
    }

}

fn output_list(file: Vec<SDFRecord>) {
    let mut i = 1;
    for record in file {
        writeln!(io::stdout(), "RECORD #{}", i).expect("Error writing to stdout");
        record.writeData();
        writeln!(io::stdout(), "").expect("Error writing to stdout");
        i = i + 1;
    }
}

fn make_table(file: Vec<BTreeMap<String, Vec<String>>>, use_headers: bool, idfield: &str, headings: Vec<&str>, fields: Vec<&str>) -> Table {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_CLEAN);
    let mut i = 1;
    if use_headers {
        let mut full_headings = vec![Cell::new("REC"), Cell::new(idfield)];
        for heading in headings {
            full_headings.push(Cell::new(heading));
        }
        table.add_row(Row::new(full_headings));
    }

    for block in file {
        let mut row = Row::new(vec![]);
        
        row.add_cell(Cell::new(&(i.to_string()))); // Adds record number
        row.add_cell(Cell::new(&block[idfield].join(","))); // Adds identifier
        // Adds all specified data
        for field in &fields {
            row.add_cell(Cell::new(&block[&field.to_string()].join(",")));
        }
        table.add_row(row);

        i = i + 1;
    }
    return table;
}

fn output_table(table: Table) {
    table.printstd();
}

fn output_csv(table: Table) {
    writeln!(io::stdout(), "{}", String::from_utf8(table.to_csv(Vec::new()).unwrap().into_inner().unwrap()).unwrap()).expect("Error writing to stdout");
}

fn output_summary(file: Vec<SDFRecord>, idfield: &str, ind_type: &str, use_headers: bool, headings: Vec<&str>, table_fields: Vec<&str>) {
    // Makes a BTreeMap that stores the records from input grouped by the specified id field 
    // This is probably a problem for unreasonably large inputs (multiple GBs)
    // This could probably be parallelized, not sure how to do it safely
    let mut records_by_id: BTreeMap<String, Vec<BTreeMap<String, Vec<String>>>> = BTreeMap::new();
    for record in file {
        let id = record.getData(idfield);
        if records_by_id.contains_key(&id) {
            records_by_id.get_mut(&id).unwrap().push(record.data);
        } else {
            records_by_id.insert(id, vec!(record.data));
        }
    }

    writeln!(io::stdout(), "\n===============================================================").expect("Error writing to stdout");
    writeln!(io::stdout(), "\nSUMMARY BY {}\n", idfield).expect("Error writing to stdout");

    let mut i = 1;
    for (id, records) in records_by_id {
        let mut fields: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut const_fields = Table::new();
        let mut var_fields = Table::new();
        for record in &records {
            for (key, val) in record {
                if fields.contains_key(key) {
                    fields.get_mut(key).unwrap().push(val.join("\n"));
                } else {
                    fields.insert(key.to_owned(), vec![val.join("\n")]);
                }
            }
        }
        for (key, vec) in fields {
            let vec_len = vec.len().to_string();
            let (min, max) = find_extremes(vec);
            if min == max {
                const_fields.add_row(Row::new(vec![Cell::new(&key), Cell::new(&min)]));
            } else {
                var_fields.add_row(Row::new(vec![Cell::new(&key), Cell::new(&("Min: ".to_string() + &min)), Cell::new(&("Max: ".to_string() + &max)), Cell::new(&("(N = ".to_string() + &vec_len + ")"))]));
            }
        }
        
        writeln!(io::stdout(), "===============================================================").expect("Error writing to stdout");
        writeln!(io::stdout(), "{} = {} (#{})\n", idfield, id, i).expect("Error writing to stdout");
        
        writeln!(io::stdout(), "Constant fields:\n").expect("Error writing to stdout");
        const_fields.set_format(*format::consts::FORMAT_CLEAN);
        const_fields.printstd();
        writeln!(io::stdout(), "").expect("Error writing to stdout");

        writeln!(io::stdout(), "Variable fields:\n").expect("Error writing to stdout");
        var_fields.set_format(*format::consts::FORMAT_CLEAN);
        var_fields.printstd();
        writeln!(io::stdout(), "").expect("Error writing to stdout");

        writeln!(io::stdout(), "Individual records:\n").expect("Error writing to stdout");
        if ind_type == "csv,table" {
            let table = make_table(records, use_headers, idfield, headings.clone(), table_fields.clone());
            output_table(table.clone());
            output_csv(table);
            writeln!(io::stdout(),"").expect("Error writing to stdout");
        } else if ind_type == "csv" {
            let table = make_table(records, use_headers, idfield, headings.clone(), table_fields.clone());
            output_csv(table);
            writeln!(io::stdout(),"").expect("Error writing to stdout");
        } else if ind_type == "table" {
            let table = make_table(records, use_headers, idfield, headings.clone(), table_fields.clone());
            output_table(table);
            writeln!(io::stdout(),"").expect("Error writing to stdout");
        } else {
            for record in records {
                for (key, val) in record {    
                    writeln!(io::stdout(),"${} eq {}", key, val.join(";")).expect("Error writing to stdout");
                }
                writeln!(io::stdout(), "").expect("Error writing to stdout");
            }
        }

        i = i + 1;
    }
}

fn find_extremes(values: Vec<String>) -> (String, String) {
    let mut is_num = true;
    let mut nums: Vec<OrderedFloat<f64>> = vec![];
    for val in &values {
        match val.parse::<f64>() {
            Ok(num) => nums.push(OrderedFloat(num)),
            Err(_) => {is_num = false; break}
        }
    }
    if is_num {
        return (nums.iter().min().unwrap().to_string(), nums.iter().max().unwrap().to_string());
    } else {
        return (values.iter().min().unwrap().to_string(), values.iter().max().unwrap().to_string());
    }
}