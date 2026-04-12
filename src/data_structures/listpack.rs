const LP_HDR_SIZE: usize = 6;
const LP_EOF: u8 = 0xFF;

const LP_ENCODING_6BIT_STR: u8 = 0x80;
const LP_ENCODING_12BIT_STR: u8 = 0xE0;
const LP_ENCODING_32BIT_STR: u8 = 0xF0;

const LP_ENCODING_13BIT_INT: u8 = 0xC0;
const LP_ENCODING_16BIT_INT: u8 = 0xF1;
const LP_ENCODING_24BIT_INT: u8 = 0xF2;
const LP_ENCODING_32BIT_INT: u8 = 0xF3;
const LP_ENCODING_64BIT_INT: u8 = 0xF4;

const LP_NUMELE_UNKNOWN: u16 = u16::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListpackValueRef<'a> {
    String(&'a [u8]),
    Int(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListpackError {
    Corrupted,
    Overflow,
}

pub struct Listpack {
    data: Vec<u8>,
}

impl Default for Listpack {
    fn default() -> Self {
        Self::new()
    }
}

impl Listpack {
    pub fn new() -> Self {
        let mut data = vec![0; LP_HDR_SIZE + 1];
        write_total_bytes(&mut data, (LP_HDR_SIZE + 1) as u32);
        write_num_elements(&mut data, 0);
        data[LP_HDR_SIZE] = LP_EOF;
        Self { data }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut lp = Self::new();
        if capacity > lp.data.capacity() {
            lp.data.reserve(capacity - lp.data.capacity());
        }
        lp
    }

    pub fn len(&self) -> usize {
        let n = read_num_elements(&self.data);
        if n != LP_NUMELE_UNKNOWN {
            return usize::from(n);
        }
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_bytes(&self) -> usize {
        read_total_bytes(&self.data) as usize
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn append(&mut self, value: &[u8]) -> Result<(), ListpackError> {
        if let Some(v) = strict_parse_i64(value) {
            self.append_int(v)
        } else {
            self.append_string(value)
        }
    }

    pub fn append_string(&mut self, value: &[u8]) -> Result<(), ListpackError> {
        let encoded = encode_string(value)?;
        self.push_encoded_entry(&encoded)
    }

    pub fn append_int(&mut self, value: i64) -> Result<(), ListpackError> {
        let encoded = encode_int(value);
        self.push_encoded_entry(&encoded)
    }

    pub fn get(&self, index: usize) -> Option<ListpackValueRef<'_>> {
        self.iter().nth(index)
    }

    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter {
            bytes: &self.data,
            offset: LP_HDR_SIZE,
        }
    }

    fn push_encoded_entry(&mut self, encoded: &[u8]) -> Result<(), ListpackError> {
        let backlen = encode_backlen(encoded.len())?;
        let additional = encoded
            .len()
            .checked_add(backlen.len())
            .ok_or(ListpackError::Overflow)?;
        let old_len = self.data.len();
        let new_len = old_len
            .checked_add(additional)
            .ok_or(ListpackError::Overflow)?;

        if new_len > u32::MAX as usize {
            return Err(ListpackError::Overflow);
        }

        self.data.pop();
        self.data.extend_from_slice(encoded);
        self.data.extend_from_slice(&backlen);
        self.data.push(LP_EOF);

        write_total_bytes(&mut self.data, new_len as u32);
        let n = read_num_elements(&self.data);
        if n != LP_NUMELE_UNKNOWN {
            if n == LP_NUMELE_UNKNOWN - 1 {
                write_num_elements(&mut self.data, LP_NUMELE_UNKNOWN);
            } else {
                write_num_elements(&mut self.data, n + 1);
            }
        }
        Ok(())
    }
}

pub struct ListpackIter<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = ListpackValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.bytes.len() || self.bytes[self.offset] == LP_EOF {
            return None;
        }

        let (value, entry_len) = decode_entry(self.bytes, self.offset).ok()?;
        self.offset += entry_len;
        Some(value)
    }
}

fn decode_entry(
    bytes: &[u8],
    offset: usize,
) -> Result<(ListpackValueRef<'_>, usize), ListpackError> {
    let first = *bytes.get(offset).ok_or(ListpackError::Corrupted)?;
    let (value, enc_len) = if first & 0x80 == 0 {
        (ListpackValueRef::Int((first & 0x7F) as i64), 1usize)
    } else if (first & 0xC0) == 0x80 {
        let slen = (first & 0x3F) as usize;
        let start = offset + 1;
        let end = start.checked_add(slen).ok_or(ListpackError::Corrupted)?;
        let s = bytes.get(start..end).ok_or(ListpackError::Corrupted)?;
        (ListpackValueRef::String(s), 1 + slen)
    } else if (first & 0xE0) == 0xC0 {
        let b1 = *bytes.get(offset + 1).ok_or(ListpackError::Corrupted)?;
        let mut u = (((first & 0x1F) as u16) << 8) | (b1 as u16);
        if u & (1 << 12) != 0 {
            u |= !0x1FFF;
        }
        (ListpackValueRef::Int((u as i16) as i64), 2)
    } else if first == LP_ENCODING_16BIT_INT {
        let s = bytes
            .get(offset + 1..offset + 3)
            .ok_or(ListpackError::Corrupted)?;
        let u = u16::from_le_bytes([s[0], s[1]]);
        (ListpackValueRef::Int((u as i16) as i64), 3)
    } else if first == LP_ENCODING_24BIT_INT {
        let s = bytes
            .get(offset + 1..offset + 4)
            .ok_or(ListpackError::Corrupted)?;
        let mut u = (s[0] as u32) | ((s[1] as u32) << 8) | ((s[2] as u32) << 16);
        if u & (1 << 23) != 0 {
            u |= 0xFF00_0000;
        }
        (ListpackValueRef::Int((u as i32) as i64), 4)
    } else if first == LP_ENCODING_32BIT_INT {
        let s = bytes
            .get(offset + 1..offset + 5)
            .ok_or(ListpackError::Corrupted)?;
        let u = u32::from_le_bytes([s[0], s[1], s[2], s[3]]);
        (ListpackValueRef::Int((u as i32) as i64), 5)
    } else if first == LP_ENCODING_64BIT_INT {
        let s = bytes
            .get(offset + 1..offset + 9)
            .ok_or(ListpackError::Corrupted)?;
        let u = u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
        (ListpackValueRef::Int(u as i64), 9)
    } else if (first & 0xF0) == LP_ENCODING_12BIT_STR {
        let b1 = *bytes.get(offset + 1).ok_or(ListpackError::Corrupted)?;
        let slen = ((((first & 0x0F) as u16) << 8) | b1 as u16) as usize;
        let start = offset + 2;
        let end = start.checked_add(slen).ok_or(ListpackError::Corrupted)?;
        let s = bytes.get(start..end).ok_or(ListpackError::Corrupted)?;
        (ListpackValueRef::String(s), 2 + slen)
    } else if first == LP_ENCODING_32BIT_STR {
        let h = bytes
            .get(offset + 1..offset + 5)
            .ok_or(ListpackError::Corrupted)?;
        let slen = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
        let start = offset + 5;
        let end = start.checked_add(slen).ok_or(ListpackError::Corrupted)?;
        let s = bytes.get(start..end).ok_or(ListpackError::Corrupted)?;
        (ListpackValueRef::String(s), 5 + slen)
    } else {
        return Err(ListpackError::Corrupted);
    };

    let backlen_len = backlen_size(enc_len);
    let backlen_start = offset + enc_len;
    let backlen_end = backlen_start
        .checked_add(backlen_len)
        .ok_or(ListpackError::Corrupted)?;
    let actual = bytes
        .get(backlen_start..backlen_end)
        .ok_or(ListpackError::Corrupted)?;
    let expected = encode_backlen(enc_len)?;
    if actual != expected.as_slice() {
        return Err(ListpackError::Corrupted);
    }
    Ok((value, enc_len + backlen_len))
}

fn encode_string(value: &[u8]) -> Result<Vec<u8>, ListpackError> {
    let len = value.len();
    let mut out = Vec::new();
    if len < 64 {
        out.push(LP_ENCODING_6BIT_STR | (len as u8));
    } else if len < 4096 {
        let hi = ((len >> 8) as u8) & 0x0F;
        out.push(LP_ENCODING_12BIT_STR | hi);
        out.push((len & 0xFF) as u8);
    } else {
        if len > u32::MAX as usize {
            return Err(ListpackError::Overflow);
        }
        out.push(LP_ENCODING_32BIT_STR);
        out.extend_from_slice(&(len as u32).to_le_bytes());
    }
    out.extend_from_slice(value);
    Ok(out)
}

fn encode_int(value: i64) -> Vec<u8> {
    if (0..=127).contains(&value) {
        return vec![value as u8];
    }
    if (-4096..=4095).contains(&value) {
        let mut v = value;
        if v < 0 {
            v += 1 << 13;
        }
        return vec![((v >> 8) as u8) | LP_ENCODING_13BIT_INT, (v & 0xFF) as u8];
    }
    if (-32768..=32767).contains(&value) {
        let mut out = vec![LP_ENCODING_16BIT_INT];
        out.extend_from_slice(&(value as i16).to_le_bytes());
        return out;
    }
    if (-8_388_608..=8_388_607).contains(&value) {
        let v = value as i32;
        return vec![
            LP_ENCODING_24BIT_INT,
            (v & 0xFF) as u8,
            ((v >> 8) & 0xFF) as u8,
            ((v >> 16) & 0xFF) as u8,
        ];
    }
    if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
        let mut out = vec![LP_ENCODING_32BIT_INT];
        out.extend_from_slice(&(value as i32).to_le_bytes());
        return out;
    }
    let mut out = vec![LP_ENCODING_64BIT_INT];
    out.extend_from_slice(&(value as u64).to_le_bytes());
    out
}

fn encode_backlen(len: usize) -> Result<Vec<u8>, ListpackError> {
    if len > 0x0FFF_FFFF {
        return Err(ListpackError::Overflow);
    }

    let mut out = vec![0; backlen_size(len)];
    let l = len as u64;
    match out.len() {
        1 => out[0] = l as u8,
        2 => {
            out[0] = (l >> 7) as u8;
            out[1] = ((l & 127) as u8) | 128;
        }
        3 => {
            out[0] = (l >> 14) as u8;
            out[1] = (((l >> 7) & 127) as u8) | 128;
            out[2] = ((l & 127) as u8) | 128;
        }
        4 => {
            out[0] = (l >> 21) as u8;
            out[1] = (((l >> 14) & 127) as u8) | 128;
            out[2] = (((l >> 7) & 127) as u8) | 128;
            out[3] = ((l & 127) as u8) | 128;
        }
        5 => {
            out[0] = (l >> 28) as u8;
            out[1] = (((l >> 21) & 127) as u8) | 128;
            out[2] = (((l >> 14) & 127) as u8) | 128;
            out[3] = (((l >> 7) & 127) as u8) | 128;
            out[4] = ((l & 127) as u8) | 128;
        }
        _ => return Err(ListpackError::Corrupted),
    }
    Ok(out)
}

fn backlen_size(len: usize) -> usize {
    if len <= 127 {
        1
    } else if len < 16383 {
        2
    } else if len < 2_097_151 {
        3
    } else if len < 268_435_455 {
        4
    } else {
        5
    }
}

fn strict_parse_i64(input: &[u8]) -> Option<i64> {
    if input.is_empty() {
        return None;
    }
    if input == b"0" {
        return Some(0);
    }

    let mut idx = 0usize;
    let mut negative = false;
    if input[0] == b'-' {
        negative = true;
        idx = 1;
    }
    if idx >= input.len() {
        return None;
    }
    if input[idx] < b'1' || input[idx] > b'9' {
        return None;
    }

    let mut val: u64 = (input[idx] - b'0') as u64;
    idx += 1;
    while idx < input.len() {
        let c = input[idx];
        if !c.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?;
        val = val.checked_add((c - b'0') as u64)?;
        idx += 1;
    }

    if negative {
        if val > (i64::MAX as u64) + 1 {
            None
        } else if val == (i64::MAX as u64) + 1 {
            Some(i64::MIN)
        } else {
            Some(-(val as i64))
        }
    } else if val > i64::MAX as u64 {
        None
    } else {
        Some(val as i64)
    }
}

fn read_total_bytes(data: &[u8]) -> u32 {
    u32::from_le_bytes([data[0], data[1], data[2], data[3]])
}

fn write_total_bytes(data: &mut [u8], v: u32) {
    data[..4].copy_from_slice(&v.to_le_bytes());
}

fn read_num_elements(data: &[u8]) -> u16 {
    u16::from_le_bytes([data[4], data[5]])
}

fn write_num_elements(data: &mut [u8], v: u16) {
    data[4..6].copy_from_slice(&v.to_le_bytes());
}

// checking

//     let mut lp = Listpack::new();
//
//     lp.append(b"hello").unwrap();
//     lp.append(b"123").unwrap();
//     lp.append(b"world").unwrap();
//     lp.append(b"-45").unwrap();
//
//     println!("len = {}", lp.len());
//
//     for (i, v) in lp.iter().enumerate() {
//         match v {
//             ListpackValueRef::String(s) => {
//                 println!("{}: str = {}", i, std::str::from_utf8(s).unwrap());
//             }
//             ListpackValueRef::Int(x) => {
//                 println!("{}: int = {}", i, x);
//             }
//         }
//     }