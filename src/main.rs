mod commands;
mod server;
mod types;
mod data_structures;

fn main() -> std::io::Result<()> {
    server::run()
}