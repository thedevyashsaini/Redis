use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use slab::Slab;
use std::io::{Read, Write};

mod commands;
use commands::parse_command;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const SERVER: Token = Token(0);
const MAX_CLEANUP: usize = 169;

pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
}

pub struct Entry {
    value: Value,
    expiry: Option<Instant>,
}

pub type DB = HashMap<Vec<u8>, Entry>;

fn main() -> std::io::Result<()> {
    println!("Starting Redis-like server on 127.0.0.1:6379");
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    let mut listener = TcpListener::bind("127.0.0.1:6379".parse().unwrap())?;

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    let mut connections = Slab::new();

    let mut db: DB = HashMap::new();
    let mut expiries: BinaryHeap<(Reverse<Instant>, Vec<u8>)> = BinaryHeap::new();

    loop {
        cleanup_expired(&mut db, &mut expiries);

        poll.poll(&mut events, None)?;

        for event in events.iter() {
            match event.token() {
                SERVER => loop {
                    match listener.accept() {
                        Ok((mut stream, _addr)) => {
                            let entry = connections.vacant_entry();
                            let token: Token = Token(entry.key() + 1);

                            poll.registry()
                                .register(&mut stream, token, Interest::READABLE)?;

                            entry.insert((stream, Vec::new(), Vec::new()));
                        }
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::WouldBlock {
                                break;
                            } else {
                                eprintln!("accept error: {}", e);
                                break;
                            }
                        }
                    }
                },

                token => {
                    let idx = token.0 - 1;

                    let mut should_remove = false;

                    if let Some((stream, r_buffer, w_buffer)) = connections.get_mut(idx) {

                        if event.is_readable() {
                            let mut temp: [u8; 1024] = [0; 1024];

                            match stream.read(&mut temp) {
                                Ok(0) => {
                                    should_remove = true;
                                }
                                Ok(n) => {
                                    r_buffer.extend_from_slice(&temp[..n]);

                                    println!("Received: {}", String::from_utf8_lossy(r_buffer));

                                    match parse_command(r_buffer) {
                                        Ok(command) => {
                                            println!("Command: {:?}", command.cmd_type);

                                            let response = command.process(&mut db, &mut expiries).unwrap();

                                            let is_empty: bool = w_buffer.is_empty();
                                            w_buffer.extend_from_slice(&response);

                                            if is_empty {
                                                poll.registry()
                                                    .reregister(stream, token, Interest::READABLE.add(Interest::WRITABLE), )?;
                                            }
                                            r_buffer.clear();
                                        }

                                        Err(_) => {
                                            let is_empty: bool = w_buffer.is_empty();
                                            w_buffer.extend_from_slice(b"-ERR invalid RESP\r\n");

                                            if is_empty {
                                                poll.registry()
                                                    .reregister(stream, token, Interest::READABLE.add(Interest::WRITABLE), )?;
                                            }
                                            r_buffer.clear();
                                        }
                                    }
                                }
                                Err(e) => {
                                    if e.kind() != std::io::ErrorKind::WouldBlock {
                                        should_remove = true;
                                    }
                                }
                            }
                        }

                        if event.is_writable() {
                            while !w_buffer.is_empty() {
                                match stream.write(w_buffer) {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        w_buffer.drain(..n);
                                    }
                                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                    Err(_) => {
                                        should_remove = true;
                                        break;
                                    }
                                }
                            }

                            if w_buffer.is_empty() {
                                poll.registry().reregister(
                                    stream,
                                    token,
                                    Interest::READABLE,
                                )?;
                            }
                        }
                    }

                    if should_remove {
                        connections.remove(idx);
                    }
                }
            }
        }
    }
}

fn cleanup_expired(
    db: &mut DB,
    expiries: &mut BinaryHeap<(Reverse<Instant>, Vec<u8>)>,
) {
    let rn = Instant::now();
    let mut cleaned: usize = 0;

    while let Some((Reverse(expiry), key)) = expiries.peek().cloned() {
        if cleaned > MAX_CLEANUP || expiry > rn {
            break;
        }

        expiries.pop();

        if let Some(Entry { expiry: Some(actual_expiry), .. }) = db.get(&key) {
            if *actual_expiry == expiry {
                db.remove(&key);
            }
        }

        cleaned +=1;
    }
}