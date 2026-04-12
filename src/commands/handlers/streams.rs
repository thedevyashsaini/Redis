use crate::command_handler;
use crate::commands::Context;
use crate::data_structures::stream::{Stream, StreamID};
use crate::types::{Entry, Key, Value};
use std::io::Write;
use std::sync::Arc;

command_handler!(xadd, args, ctx, {
    let key = args.get(0).ok_or(b"-ERR missing key".to_vec())?;
    let entry_id = args.get(1).ok_or(b"-ERR missing entry id".to_vec())?;

    if entry_id == b"0-0" {
        return Err(b"-ERR The ID specified in XADD must be greater than 0-0\r\n".to_vec());
    }

    let fields: Vec<(&[u8], &[u8])>  = {
        let kv = &args[2..];

        if kv.len() % 2 != 0 {
            return Err(b"-ERR wrong number of arguments".to_vec());
        }

        let pairs: Vec<(&[u8], &[u8])> = kv
            .chunks_exact(2)
            .map(|chunk| (chunk[0], chunk[1]))
            .collect();

        pairs
    };

    let entry_id_formatted = StreamID::parse(entry_id)?;

    if let Some(Entry {
        value: Value::Stream(ref mut stream),
        ..
    }) = ctx.db.get_mut(*key)
    {
        stream.add(entry_id_formatted, &fields)?;
    } else {
        let key: Key = Arc::from(*key);
        let mut newstream = Stream::new();
        newstream.add(entry_id_formatted, &fields)?;
        ctx.db.insert(
            key.clone(),
            Entry {
                value: Value::Stream(newstream),
                expiry: None,
            },
        );
    }

    let mut res = Vec::with_capacity(entry_id.len() + 25);
    write!(res, "${}\r\n", entry_id.len()).unwrap();
    res.extend_from_slice(entry_id);
    write!(res, "\r\n").unwrap();
    Ok(res)
});
