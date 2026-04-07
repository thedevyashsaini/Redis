use mio::Token;
use std::collections::HashMap;

use crate::types::{Expiries, PubSub, DB};

mod handlers;
mod parser;

pub use parser::{normalize_upper, parse_command};

type CommandHandler = fn(args: &[&[u8]], &mut Context) -> Result<Vec<u8>, Vec<u8>>;

pub enum Action {
    Publish { channel: Vec<u8>, message: Vec<u8> },
}

pub struct Context<'a> {
    pub db: &'a mut DB,
    pub expiries: &'a mut Expiries,
    pub pubsub: &'a mut PubSub,
    pub subscriptions: &'a mut Vec<Vec<u8>>,
    pub is_pubsub: &'a mut bool,
    pub token: Token,
    pub actions: &'a mut Vec<Action>,
}

pub type CommandTable = HashMap<&'static [u8], CommandHandler>;

pub fn command_table() -> CommandTable {
    let mut table: CommandTable = HashMap::new();
    table.insert(b"PING", handlers::core::ping);
    table.insert(b"ECHO", handlers::core::echo);
    table.insert(b"SET", handlers::strings::set);
    table.insert(b"GET", handlers::strings::get);
    table.insert(b"RPUSH", handlers::lists::rpush);
    table.insert(b"LRANGE", handlers::lists::lrange);
    table.insert(b"LPUSH", handlers::lists::lpush);
    table.insert(b"LLEN", handlers::lists::llen);
    table.insert(b"LPOP", handlers::lists::lpop);
    table.insert(b"BLPOP", handlers::lists::blpop);
    table.insert(b"SUBSCRIBE", handlers::pubsub::subscribe);
    table.insert(b"PUBLISH", handlers::pubsub::publish);
    table.insert(b"UNSUBSCRIBE", handlers::pubsub::unsubscribe);
    table.insert(b"TYPE", handlers::core::typee);
    table
}
