use crate::data_structures::listpack::{Listpack, ListpackValueRef};
use crate::data_structures::radix::RadixTree;
use std::collections::BTreeMap;



#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct StreamID {
    pub ms: u64,
    pub seq: u64,
}

impl StreamID {
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.ms.to_be_bytes());
        out[8..].copy_from_slice(&self.seq.to_be_bytes());
        out
    }

    pub fn parse(input: &[u8]) -> Result<Self, Vec<u8>> {
        let dash = input.iter().position(|&b| b == b'-')
            .ok_or(b"-ERR invalid stream id".to_vec())?;

        let (ms_part, seq_part) = input.split_at(dash);
        let seq_part = &seq_part[1..];

        let ms = parse_u64(ms_part)?;
        let seq = parse_u64(seq_part)?;

        Ok(StreamID { ms, seq })
    }
}

fn parse_u64(bytes: &[u8]) -> Result<u64, Vec<u8>> {
    if bytes.is_empty() {
        return Err(b"-ERR invalid integer".to_vec());
    }

    let mut num = 0u64;

    for &b in bytes {
        if !(b'0'..=b'9').contains(&b) {
            return Err(b"-ERR invalid integer".to_vec());
        }
        num = num * 10 + (b - b'0') as u64;
    }

    Ok(num)
}

pub struct StreamNode {
    pub last_id: StreamID,
    pub lp: Listpack,
}

pub struct Stream {
    tree: RadixTree<StreamNode>,
    index: BTreeMap<StreamID, ()>,
    last_id: Option<StreamID>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            tree: RadixTree::new(),
            index: BTreeMap::new(),
            last_id: None
        }
    }

    pub fn add(&mut self, id: StreamID, fields: &[(&[u8], &[u8])]) -> Result<(), Vec<u8>>{
        if let Some(last) = self.last_id {
            if id <= last {
                return Err(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".to_vec());
            }
        }
        if let Some(prev_key) = self.get_floor_key(&id) {
            let key_bytes = prev_key.to_bytes();

            if let Some(node) = self.tree.get(&key_bytes) {
                if can_append(node, &id) {
                    append_entry(&mut node.lp, &id, &fields);
                    node.last_id = id;
                    self.last_id = Some(id);
                    return Ok(());
                }
            }
        }

        let mut lp = Listpack::new();
        append_entry(&mut lp, &id, &fields);

        let node = StreamNode {
            last_id: id,
            lp,
        };

        let key_bytes = id.to_bytes();

        self.tree.insert(&key_bytes, node);
        self.index.insert(id, ());
        self.last_id = Some(id);
        Ok(())
    }

    pub fn get(&mut self, id: StreamID) -> Option<Vec<ListpackValueRef<'_>>> {
        let base_id = self.get_floor_key(&id)?;

        let node = self.tree.get(&base_id.to_bytes())?;

        find_entry_in_listpack(&node.lp, &id)
    }

    fn get_floor_key(&self, id: &StreamID) -> Option<StreamID> {
        self.index.range(..=id).next_back().map(|(k, _)| *k)
    }
}

fn find_entry_in_listpack<'a>(
    lp: &'a Listpack,
    target: &StreamID,
) -> Option<Vec<ListpackValueRef<'a>>> {
    let mut iter = lp.iter();

    while let Some(ListpackValueRef::String(ms_bytes)) = iter.next() {
        let seq_bytes = match iter.next()? {
            ListpackValueRef::String(b) => b,
            _ => return None,
        };

        let ms = u64::from_be_bytes(ms_bytes.try_into().ok()?);
        let seq = u64::from_be_bytes(seq_bytes.try_into().ok()?);

        let num_fields = match iter.next()? {
            ListpackValueRef::Int(n) => n as usize,
            ListpackValueRef::String(b) => {
                u64::from_be_bytes(b.try_into().ok()?) as usize
            }
        };

        let mut fields = Vec::new();

        for _ in 0..num_fields {
            let f = iter.next()?;
            let v = iter.next()?;
            fields.push(f);
            fields.push(v);
        }

        if ms == target.ms && seq == target.seq {
            return Some(fields);
        }
    }

    None
}

fn append_entry(
    lp: &mut Listpack,
    id: &StreamID,
    fields: &[(&[u8], &[u8])],
) {
    lp.append(&id.ms.to_be_bytes()).unwrap();
    lp.append(&id.seq.to_be_bytes()).unwrap();

    lp.append(&(fields.len() as u64).to_be_bytes()).unwrap();

    for (f, v) in fields {
        lp.append(f).unwrap();
        lp.append(v).unwrap();
    }
}

fn can_append(node: &StreamNode, id: &StreamID) -> bool {
    let size_ok = node.lp.total_bytes() < 4096;
    let strictly_increasing = *id > node.last_id;
    let id_close = id.ms >= node.last_id.ms &&
        id.ms - node.last_id.ms < 1000;

    size_ok && strictly_increasing && id_close
}