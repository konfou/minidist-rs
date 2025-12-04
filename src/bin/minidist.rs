use clap::{Parser, Subcommand};

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
            println!("INIT:");
            println!("  dir = {}", dir);
            println!("  schema = {}", schema);
        }

        Commands::Load {
            dir,
            csv,
            sort_key,
            segments,
        } => {
            println!("LOAD:");
            println!("  dir = {}", dir);
            println!("  csv = {}", csv);
            println!("  sort_key = {}", sort_key);
            println!("  segments = {}", segments);
        }

        Commands::Schema { sub } => match sub {
            SchemaCommands::Show { dir } => {
                println!("SCHEMA SHOW:");
                println!("  dir = {}", dir);
            }
        },

        Commands::Info { dir } => {
            println!("INFO:");
            println!("  dir = {}", dir);
        }
    }
}
