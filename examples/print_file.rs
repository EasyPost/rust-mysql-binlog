use std::io::Write;

use mysql_binlog;
use serde_json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} /path/to/binlog/file", args[0]);
        std::process::exit(2);
    }
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    for event in mysql_binlog::parse_file(&args[1])? {
        if let Ok(event) = event {
            serde_json::to_writer_pretty(&mut stdout, &event).map_err(|e| Box::new(e))?;
            write!(stdout, "\n")?;
        }
    }
    Ok(())
}
