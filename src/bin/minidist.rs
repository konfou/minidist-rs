use clap::{Parser, Subcommand};
use minidist_rs::{init_table, inspect_metadata, inspect_schema};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "minidist")]
#[command(about = "minidist-rs storage manager", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {
        dir: String,

        #[arg(long)]
        schema: String,
    },

    Load {
        dir: String,

        #[arg(long)]
        csv: String,

        #[arg(long, value_name = "ID")]
        sort_key: String,

        #[arg(long, value_name = "N")]
        segments: u32,
    },

    Schema {
        #[command(subcommand)]
        sub: SchemaCommands,
    },

    Info {
        dir: String,
    },
}

#[derive(Subcommand)]
enum SchemaCommands {
    Show { dir: String },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { dir, schema } => {
            let dir = PathBuf::from(dir);
            let schema = PathBuf::from(schema);

            match init_table(&dir, &schema) {
                Ok(()) => println!("Initialized table {:?}", dir),
                Err(e) => eprintln!("Error: {}", e),
            }
        }

        Commands::Load {
            dir,
            csv,
            sort_key,
            segments,
        } => {
            let dir = PathBuf::from(dir);
            let csv = PathBuf::from(csv);

            let schema_text = std::fs::read_to_string(dir.join("_schema.ssf"))
                .map_err(|e| format!("Schema missing: {}", e))
                .unwrap();

            let schema = minidist_rs::storage_schema::parse_schema_file(&schema_text).unwrap();

            match minidist_rs::storage_load::load_table(
                &dir,
                &csv,
                &sort_key,
                segments as usize,
                &schema,
            ) {
                Ok(()) => println!("Loaded CSV into {} segments", segments),
                Err(e) => eprintln!("Error: {}", e),
            }
        }

        Commands::Schema { sub } => match sub {
            SchemaCommands::Show { dir } => {
                let path = PathBuf::from(dir);
                match inspect_schema(&path) {
                    Ok(contents) => print!("{}", contents),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
        },

        Commands::Info { dir } => {
            let path = PathBuf::from(dir);
            match inspect_metadata(&path) {
                Ok(contents) => print!("{}", contents),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}
