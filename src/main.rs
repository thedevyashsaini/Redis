use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use slab::Slab;
use std::io::{Read, Write};

mod commands;
use commands::command_table;
use commands::normalize_upper;
use commands::parse_command;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

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
pub type Expiries = BinaryHeap<(Reverse<Instant>, Vec<u8>)>;



fn main() -> std::io::Result<()> {
    println!("Starting Redis-like server on 127.0.0.1:6379");
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    let mut listener = TcpListener::bind("127.0.0.1:6379".parse().unwrap())?;

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    let mut connections = Slab::new();

    let mut db: DB = HashMap::new();
    let mut expiries: Expiries = BinaryHeap::new();
    let mut blocked_queues: HashMap<Vec<u8>, VecDeque<Token>> = HashMap::new();
    let mut blocked_timeouts: BinaryHeap<(Reverse<Instant>, Token)> = BinaryHeap::new();

    let table = command_table();

    loop {
        cleanup_expired(&mut db, &mut expiries);
        cleanup_blocked(&mut connections, &mut blocked_timeouts, &mut poll);

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

                            entry.insert((stream, Vec::new(), Vec::new(), false, None, None));
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
                    let mut wake_key: Option<Vec<u8>> = None;

                    if let Some((stream, r_buffer, w_buffer, blocked_flag, block_key, block_deadline)) = connections.get_mut(idx) {
                        if event.is_readable() {
                            if *blocked_flag {
                                continue;
                            }

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
                                            let mut temp = [0u8; 32];
                                            let normalized =
                                                normalize_upper(command.name, &mut temp);

                                            let is_empty: bool = w_buffer.is_empty();

                                            match table.get(normalized) {
                                                Some(handler) => {
                                                    match (handler)(&command.args, &mut db, &mut expiries) {
                                                        Ok(bytes) => {
                                                            w_buffer.extend_from_slice(&bytes);

                                                            if normalized == b"LPUSH" || normalized == b"RPUSH" {
                                                                if let Some(key) = command.args.get(0) {
                                                                    wake_key = Some(key.to_vec());
                                                                }
                                                            }
                                                        }
                                                        Err(bytes) => {
                                                            if bytes == b"__BLOCK__" {
                                                                *blocked_flag = true;

                                                                let key = command.args[0].to_vec();

                                                                *block_key = Some(key.clone());

                                                                let timeout = std::str::from_utf8(command.args[1])
                                                                    .unwrap()
                                                                    .parse::<u64>()
                                                                    .unwrap();

                                                                if timeout > 0 {
                                                                    let deadline = Instant::now()
                                                                        + std::time::Duration::from_secs(timeout);

                                                                    *block_deadline = Some(deadline);
                                                                    blocked_timeouts.push((Reverse(deadline), token));
                                                                }

                                                                blocked_queues
                                                                    .entry(key)
                                                                    .or_insert_with(VecDeque::new)
                                                                    .push_back(token);

                                                                continue;
                                                            } else {
                                                                w_buffer.extend_from_slice(&bytes);
                                                            }
                                                        }
                                                    }
                                                },

                                                None => {
                                                    w_buffer.extend_from_slice(
                                                        b"-ERR unknown command\r\n",
                                                    );
                                                }
                                            }

                                            if is_empty {
                                                poll.registry().reregister(
                                                    stream,
                                                    token,
                                                    Interest::READABLE.add(Interest::WRITABLE),
                                                )?;
                                            }
                                            r_buffer.clear();
                                        }

                                        Err(_) => {
                                            let is_empty: bool = w_buffer.is_empty();
                                            w_buffer.extend_from_slice(b"-ERR invalid RESP\r\n");

                                            if is_empty {
                                                poll.registry().reregister(
                                                    stream,
                                                    token,
                                                    Interest::READABLE.add(Interest::WRITABLE),
                                                )?;
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
                                poll.registry()
                                    .reregister(stream, token, Interest::READABLE)?;
                            }
                        }
                    }

                    if let Some(key) = wake_key {
                        wake_client(
                            &key,
                            &mut db,
                            &mut blocked_queues,
                            &mut connections,
                            &mut poll,
                        )?;
                    }

                    if should_remove {
                        connections.remove(idx);
                    }
                }
            }
        }
    }
}

fn cleanup_expired(db: &mut DB, expiries: &mut Expiries) {
    let rn = Instant::now();
    let mut cleaned: usize = 0;

    while let Some((Reverse(expiry), key)) = expiries.peek().cloned() {
        if cleaned > MAX_CLEANUP || expiry > rn {
            break;
        }

        expiries.pop();

        if let Some(Entry {
            expiry: Some(actual_expiry),
            ..
        }) = db.get(&key)
        {
            if *actual_expiry == expiry {
                db.remove(&key);
            }
        }

        cleaned += 1;
    }
}

fn wake_client(
    key: &[u8],
    db: &mut DB,
    blocked_queues: &mut HashMap<Vec<u8>, VecDeque<Token>>,
    connections: &mut Slab<(mio::net::TcpStream, Vec<u8>, Vec<u8>, bool, Option<Vec<u8>>, Option<Instant>)>,
    poll: &mut Poll,
) -> std::io::Result<()> {
    if let Some(queue) = blocked_queues.get_mut(key) {
        while let Some(token) = queue.pop_front() {
            if let Some((stream, _, w_buffer, blocked, block_key, block_deadline)) =
                connections.get_mut(token.0 - 1)
            {

                if !*blocked {
                    continue;
                }

                *blocked = false;
                *block_key = None;
                *block_deadline = None;

                if let Some(Entry {
                                value: Value::List(ref mut list),
                                ..
                            }) = db.get_mut(key)
                {
                    if let Some(val) = list.pop_front() {
                        let mut res = Vec::with_capacity(val.len() + key.len() + 64);

                        write!(res, "*2\r\n")?;

                        write!(res, "${}\r\n", key.len())?;
                        res.extend_from_slice(key);
                        res.extend_from_slice(b"\r\n");

                        write!(res, "${}\r\n", val.len())?;
                        res.extend_from_slice(&val);
                        res.extend_from_slice(b"\r\n");

                        let was_empty = w_buffer.is_empty();
                        w_buffer.extend_from_slice(&res);

                        if was_empty {
                            poll.registry().reregister(
                                stream,
                                token,
                                Interest::READABLE.add(Interest::WRITABLE),
                            )?;
                        }
                    }
                }

                break;
            }
        }
    }

    Ok(())
}

fn cleanup_blocked(
    connections: &mut Slab<(mio::net::TcpStream, Vec<u8>, Vec<u8>, bool, Option<Vec<u8>>, Option<Instant>)>,
    blocked_timeouts: &mut BinaryHeap<(Reverse<Instant>, Token)>,
    poll: &mut Poll,
) {
    let now = Instant::now();

    while let Some((Reverse(t), token)) = blocked_timeouts.peek().cloned() {
        if t > now {
            break;
        }

        blocked_timeouts.pop();

        if let Some((stream, _, w_buffer, blocked, block_key, block_deadline)) =
            connections.get_mut(token.0 - 1)
        {
            if *blocked {
                *blocked = false;
                *block_key = None;
                *block_deadline = None;

                let was_empty = w_buffer.is_empty();

                w_buffer.extend_from_slice(b"$-1\r\n");

                if was_empty {
                    poll.registry().reregister(
                        stream,
                        token,
                        Interest::READABLE.add(Interest::WRITABLE),
                    ).unwrap();
                }
            }
        }
    }
}