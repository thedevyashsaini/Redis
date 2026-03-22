pub fn read_line(buf: &[u8], start: usize) -> Option<(String, usize)> {
    for i in start..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            let line: String = std::str::from_utf8(&buf[start..i]).ok()?.to_string();
            return Some((line, i + 2));
        }
    }
    None
}

pub fn parse_bulk_string(buf: &[u8], start: usize) -> Option<(String, usize)> {
    if buf.get(start)? != &b'$' {
        return None;
    }

    let (len_str, mut pos) = read_line(buf, start + 1)?;
    let len: usize = len_str.parse().ok()?;

    let end = pos + len;
    if end + 2 > buf.len() {
        return None;
    }

    let data: String = std::str::from_utf8(&buf[pos..end]).ok()?.to_string();
    pos = end + 2;

    Some((data, pos))
}

#[derive(Debug)]
pub enum CommandType {
    PING,
    ECHO
}

#[derive(Debug)]
pub struct Command<'a> {
    pub cmd_type: CommandType,
    args:  (usize, &'a [u8], usize)
}

impl Command<'_> {
    pub(crate) fn process(&mut self) -> Result<String, String> {
        match self.cmd_type  {
            CommandType::PING => {
                if self.args.0 > 0 {
                    let arg = self.get_next_arg().unwrap();
                    Ok(format!("${}\r\n{}\r\n", arg.len(), arg))
                } else {
                    Ok("+PONG\r\n".to_string())
                }
            }
            CommandType::ECHO => {
                if self.args.0 < 1 {
                    Err("-ERR wrong number of arguments\r\n".to_string())
                } else {
                    let arg = self.get_next_arg().unwrap();
                    Ok(format!("${}\r\n{}\r\n", arg.len(), arg))
                }
            }
        }
    }

    fn get_next_arg(&mut self) -> Result<String, String> {
        match parse_bulk_string(self.args.1, self.args.2) {
            Some(t) => { self.args.2 = t.1; Ok(t.0) },
            None => Err("Failed to parse bulk string".to_string()),
        }
    }
}

pub fn parse_command(buf: &[u8]) -> Result<Command<'_>, String> {

    if buf.get(0).unwrap() != &b'*' {
        return Err("No Command".to_string());
    }

    let (count_str, mut pos) = read_line(buf, 1).unwrap();
    let num: usize = count_str.parse().unwrap();

    let (cmd, new_pos) = parse_bulk_string(buf, pos).unwrap();
    pos = new_pos;

    let command = Command {
        cmd_type: match cmd.to_uppercase().as_str() {
            "PING" => CommandType::PING,
            "ECHO" => CommandType::ECHO,
            _ => return Err(format!("Invalid command: {}", cmd)),
        },
        args: (num - 1, buf, pos)
    };

    Ok(command)
}