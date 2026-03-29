use std::cmp::Reverse;
use std::collections::{VecDeque, HashMap};
use std::io::Write;
use std::time::Instant;

use crate::{Entry, Expiries, Value, DB};

pub fn read_line(buf: &[u8], start: usize) -> Option<(usize, usize)> {
    for i in start..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((start, i + 2));
        }
    }
    None
}

pub fn parse_bulk_string(buf: &[u8], start: usize) -> Option<(&[u8], usize)> {
    if buf.get(start)? != &b'$' {
        return None;
    }

    let (len_start, len_end) = read_line(buf, start + 1)?;
    let len = std::str::from_utf8(&buf[len_start..len_end - 2])
        .ok()?
        .parse::<usize>()
        .ok()?;

    let data_start = len_end;
    let data_end = data_start + len;

    if data_end + 2 > buf.len() {
        return None;
    }

    Some((&buf[data_start..data_end], data_end + 2))
}

pub fn normalize_upper<'a>(cmd: &[u8], buf: &'a mut [u8]) -> &'a [u8] {
    for (i, b) in cmd.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }
    &buf[..cmd.len()]
}

type CommandHandler = fn(
    args: &[&[u8]],
    db: &mut DB,
    expiries: &mut Expiries,
) -> Result<Vec<u8>, Vec<u8>>;

macro_rules! command_handler {
    ($name:ident, $args:ident, $db:ident, $exp:ident, $body:block) => {
        fn $name(
            $args: &[&[u8]],
            $db: &mut DB,
            $exp: &mut Expiries,
        ) -> Result<Vec<u8>, Vec<u8>> $body
    };
}

command_handler!(ping, args, _db, _expiries, {
    if !args.is_empty() {
        let arg = args[0];
        let mut res = Vec::with_capacity(arg.len() + 32);
        write!(res, "${}\r\n", arg.len()).unwrap();
        res.extend_from_slice(arg);
        res.extend_from_slice(b"\r\n");
        Ok(res)
    } else {
        Ok(b"+PONG\r\n".to_vec())
    }
});

command_handler!(echo, args, _db, _expiries, {
    if args.is_empty() {
        Err(b"-ERR wrong number of arguments\r\n".to_vec())
    } else {
        let arg = args[0];
        let mut res = Vec::with_capacity(arg.len() + 32);
        write!(res, "${}\r\n", arg.len()).unwrap();
        res.extend_from_slice(arg);
        res.extend_from_slice(b"\r\n");
        Ok(res)
    }
});

command_handler!(set, args, db, expiries, {
    let key = args.get(0).ok_or(b"ERR missing key".to_vec())?;
    let value = args.get(1).ok_or(b"ERR missing value".to_vec())?;

    let mut expiry: Option<Instant> = None;

    let rn = Instant::now();
    if args.len() > 2 {
        let option = std::str::from_utf8(args[2]).unwrap().to_uppercase();

        if option == "EX" || option == "PX" {
            let exp = std::str::from_utf8(
                args.get(3).ok_or(b"ERR missing EX value".to_vec())?
            )
                .unwrap()
                .parse::<u64>()
                .map_err(|_| b"ERR invalid EX/PX value".to_vec())?;

            let duration = if option == "PX" {
                std::time::Duration::from_millis(exp)
            } else {
                std::time::Duration::from_secs(exp)
            };

            expiry = Some(rn + duration);
        }
    }

    if let Some(exp) = expiry {
        expiries.push((Reverse(exp), key.to_vec()));
    }

    db.insert(
        key.to_vec(),
        Entry {
            value: Value::String(value.to_vec()),
            expiry,
        },
    );

    Ok(b"+OK\r\n".to_vec())
});

command_handler!(get, args, db, _expiries, {
   let key = args.get(0).ok_or(b"ERR missing key".to_vec())?;

    if let Some(entry) = db.get(*key) {
        if let Some(exp) = entry.expiry {
            if Instant::now() >= exp {
                db.remove(*key);
                return Ok(b"$-1\r\n".to_vec());
            }
        }

        match &entry.value {
            Value::String(val) => {
                let mut res = Vec::with_capacity(val.len() + 32);
                write!(res, "${}\r\n", val.len()).unwrap();
                res.extend_from_slice(val);
                res.extend_from_slice(b"\r\n");
                Ok(res)
            }
            _ => Err(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec()),
        }
    } else {
        Ok(b"$-1\r\n".to_vec())
    }
});

command_handler!(rpush, args, db, _expiries, {
    let key = args.get(0).ok_or(b"ERR missing key".to_vec())?;
    if args.len() < 2 {
        return Err(b"-ERR wrong number of arguments\r\n".to_vec());
    }

    let values = &args[1..];

    let list_len: usize;

    if let Some(Entry {
                    value: Value::List(ref mut list),
                    ..
                }) = db.get_mut(*key)
    {
        for v in values {
            list.push_back(v.to_vec());
        }
        list_len = list.len();
    } else {
        let mut newlist = VecDeque::new();
        for v in values {
            newlist.push_back(v.to_vec());
        }

        list_len = newlist.len();

        db.insert(
            key.to_vec(),
            Entry {
                value: Value::List(newlist),
                expiry: None,
            },
        );
    }

    let mut res = Vec::with_capacity(32);
    write!(res, ":{}\r\n", list_len).unwrap();
    Ok(res)
});

command_handler!(lrange, args, db, _expiries, {
    let key = args.get(0).ok_or(b"ERR missing key".to_vec())?;
    let start = args.get(1).ok_or(b"ERR missing start".to_vec())?;
    let stop = args.get(2).ok_or(b"ERR missing stop".to_vec())?;

    if let Some(entry) = db.get(*key) {
        match &entry.value {
            Value::List(list) => {
                let start = std::str::from_utf8(start).unwrap().parse::<isize>().unwrap();
                let stop = std::str::from_utf8(stop).unwrap().parse::<isize>().unwrap();

                let len = list.len() as isize;

                let mut start = if start < 0 { len + start } else { start };
                let mut stop = if stop < 0 { len + stop } else { stop };

                if start < 0 { start = 0; }
                if stop >= len { stop = len - 1; }

                if start > stop || start >= len {
                    return Ok(b"*0\r\n".to_vec());
                }

                let start = start as usize;
                let count = stop as usize - start + 1;

                let mut res = Vec::with_capacity(count * 16);

                write!(res, "*{}\r\n", count).unwrap();

                for item in list.iter().skip(start).take(count) {
                    write!(res, "${}\r\n", item.len()).unwrap();
                    res.extend_from_slice(item);
                    res.extend_from_slice(b"\r\n");
                }

                Ok(res)
            },
            _ => Err(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec()),
        }
    } else {
        Ok(b"*0\r\n".to_vec())
    }
});

command_handler!(lpush, args, db, _expiries, {
    let key = args.get(0).ok_or(b"ERR missing key".to_vec())?;
    if args.len() < 2 {
        return Err(b"-ERR wrong number of arguments\r\n".to_vec());
    }

    let values = &args[1..];

    let list_len: usize;

    if let Some(Entry {
                    value: Value::List(ref mut list),
                    ..
                }) = db.get_mut(*key)
    {
        for v in values {
            list.push_front(v.to_vec());
        }
        list_len = list.len();
    } else {
        let mut newlist = VecDeque::new();
        for v in values {
            newlist.push_front(v.to_vec());
        }

        list_len = newlist.len();

        db.insert(
            key.to_vec(),
            Entry {
                value: Value::List(newlist),
                expiry: None,
            },
        );
    }

    let mut res = Vec::with_capacity(32);
    write!(res, ":{}\r\n", list_len).unwrap();
    Ok(res)
});

pub type CommandTable = HashMap<&'static [u8], CommandHandler>;

pub fn command_table() -> CommandTable {
    let mut table: CommandTable = HashMap::new();

    table.insert(b"PING", ping);
    table.insert(b"ECHO", echo);
    table.insert(b"SET", set);
    table.insert(b"GET", get);
    table.insert(b"RPUSH", rpush);
    table.insert(b"LRANGE", lrange);
    table.insert(b"LPUSH", lpush);
    table
}

pub struct Command<'a> {
    pub name: &'a [u8],
    pub args: Vec<&'a [u8]>,
}

pub fn parse_command(buf: &[u8]) -> Result<Command<'_>, Vec<u8>> {
    if buf.get(0) != Some(&b'*') {
        return Err(b"No Command".to_vec());
    }

    let (count_start, count_end) = read_line(buf, 1).ok_or("Invalid")?;
    let count = std::str::from_utf8(&buf[count_start..count_end - 2])
        .map_err(|_| "Invalid")?
        .parse::<usize>()
        .map_err(|_| "Invalid")?;

    let mut pos = count_end;

    let (cmd_bytes, new_pos) = parse_bulk_string(buf, pos).ok_or("Invalid")?;
    pos = new_pos;

    let mut args = Vec::with_capacity(count - 1);

    for _ in 0..count - 1 {
        let (arg, new_pos) = parse_bulk_string(buf, pos).ok_or("Invalid")?;
        args.push(arg);
        pos = new_pos;
    }

    Ok(Command { name: cmd_bytes, args })
}
