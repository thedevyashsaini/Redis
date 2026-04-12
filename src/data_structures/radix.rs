#[derive(Debug)]
struct RadixNode<V> {
    prefix: Vec<u8>,
    children: Vec<(u8, RadixNode<V>)>,
    value: Option<V>,
}

#[derive(Debug)]
pub struct RadixTree<V> {
    root: RadixNode<V>,
}

// Legends (TDS, Shree): https://i.ibb.co/Z6C42nBn/20260412-104952.jpg

impl<V> RadixTree<V> {
    pub fn new() -> Self {
        Self {
            root: RadixNode {
                prefix: Vec::new(),
                children: Vec::new(),
                value: None,
            },
        }
    }

    pub fn insert(&mut self, key: &[u8], value: V) {
        insert_rec(&mut self.root, key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        get_rec(&self.root, key)
    }
}

fn get_rec<'a, V>(node: &'a RadixNode<V>, key: &[u8]) -> Option<&'a V> {
    if !key.starts_with(&node.prefix) {
        return None;
    }

    let rem = &key[node.prefix.len()..];

    if rem.is_empty() {
        return node.value.as_ref();
    }

    let next = rem[0];
    for (b, child) in &node.children {
        if *b == next {
            return get_rec(child, rem);
        }
    }

    None
}

fn insert_rec<V>(node: &mut RadixNode<V>, key: &[u8], value: V) {
    let common = lcp(&node.prefix, key);

    if common < node.prefix.len() {
        let mut child = RadixNode {
            prefix: node.prefix[common..].to_vec(),
            children: std::mem::take(&mut node.children),
            value: node.value.take(),
        };

        node.prefix.truncate(common);
        node.children = vec![(child.prefix[0], child)];
        node.value = None;
    }

    if common == key.len() {
        node.value = Some(value);
        return;
    }

    let next = key[common];

    for (b, child) in node.children.iter_mut() {
        if *b == next {
            insert_rec(child, &key[common..], value);
            return;
        }
    }

    let new_node = RadixNode {
        prefix: key[common..].to_vec(),
        children: Vec::new(),
        value: Some(value),
    };

    node.children.push((next, new_node));
    node.children.sort_by_key(|(b, _)| *b);
}

fn lcp(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    for i in 0..len {
        if a[i] != b[i] {
            return i;
        }
    }
    len
}

// checking

// let mut treee: RadixTree<usize> = RadixTree::new();
//
// treee.insert(b"hii", 19);
// treee.insert(b"hiia", 17);
// treee.insert(b"hiiab", 31);
// treee.insert(b"hc", 12);
//
// println!("{:?}", treee); // should print the tree
// println!("{:?}", treee.get(b"hc")); // should print some 12