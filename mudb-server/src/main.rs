use cap_tempfile::TempDir;
use clap::{Parser, Subcommand};
use mudb::Mudb;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, value_name = "FILE")]
    data_dir: Option<PathBuf>,

    #[clap(subcommand)]
    command: Option<Op>,
}

#[derive(Subcommand)]
enum Op {
    Check,
}

fn main() {
    let args = Args::parse();

    match &args.command {
        Some(Op::Check) => {
            let tmpd = TempDir::new(cap_std::ambient_authority()).unwrap();
            let _ = tmpd.create_dir("tmp").unwrap();
            let data = tmpd.open_dir("tmp").unwrap();
            let db = Mudb::<String>::open(Rc::new(data), "check");
        }
        None => {}
    }
}
