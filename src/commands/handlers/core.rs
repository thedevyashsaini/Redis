use crate::command_handler;
use crate::commands::Context;
use std::io::Write;
use crate::types::{Entry, Value};

command_handler!(ping, args, ctx, {
    if !args.is_empty() {
        let arg = args[0];
        let mut res = Vec::with_capacity(arg.len() + 32);

        if *ctx.is_pubsub {
            res.extend_from_slice(b"*2\r\n$4\r\npong\r\n");
        }
        write!(res, "${}\r\n", arg.len()).unwrap();
        res.extend_from_slice(arg);
        res.extend_from_slice(b"\r\n");
        Ok(res)
    } else {
        if *ctx.is_pubsub {
            return Ok(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n".to_vec());
        }
        Ok(b"+PONG\r\n".to_vec())
    }
});

command_handler!(echo, args, _ctx, {
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

command_handler!(typee, args, ctx, {
    let key = args.get(0).ok_or(b"-ERR missing key".to_vec())?;
    if let Some(Entry { value, ..}) = ctx.db.get(*key) {
        let ret = match value {
            Value::List(_) =>  b"+list\r\n".to_vec(),
            Value::String(_) => b"+string\r\n".to_vec(),
        };
        let mut res = Vec::with_capacity(9);
        res.extend_from_slice(&*ret);
        return Ok(res);
    }
    Err(b"+none\r\n".to_vec())
});
