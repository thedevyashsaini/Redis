mod commands;
mod server;
mod types;

fn main() -> std::io::Result<()> {
    server::run()
}
