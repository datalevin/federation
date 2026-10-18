//! Independent, dependency-free verifier for the experimental golden subset.
//! This is a fixture oracle, not a production Rust decoder or stability claim.
use std::{cmp::Ordering, collections::BTreeMap, env, fs};

#[derive(Clone, Debug, PartialEq)]
enum Node {
    Long(i64), Text(String), Keyword(String, String), Uuid([u8; 16]),
    Bytes(Vec<u8>), Decimal(i32, String), Vector(Vec<Node>), Map(Vec<(Node, Node)>),
}
use Node::*;

fn kw(s: &str) -> Node {
    let parts: Vec<&str> = s.splitn(2, '/').collect();
    if parts.len() == 2 { Keyword(parts[0].into(), parts[1].into()) }
    else { Keyword(String::new(), s.into()) }
}
fn map(entries: Vec<(&str, Node)>) -> Node {
    Map(entries.into_iter().map(|(k, v)| (kw(k), v)).collect())
}
fn text(s: &str) -> Node { Text(s.into()) }
fn bytes_cmp(a: &[u8], b: &[u8]) -> Ordering { a.len().cmp(&b.len()).then(a.cmp(b)) }
fn frame(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
}
fn encode(node: &Node) -> Vec<u8> {
    let mut out = Vec::new();
    match node {
        Long(n) => { out.push(3); out.extend_from_slice(&n.to_be_bytes()); }
        Text(s) => { out.push(9); frame(&mut out, s.as_bytes()); }
        Keyword(ns, name) => {
            out.push(10); frame(&mut out, ns.as_bytes()); frame(&mut out, name.as_bytes());
        }
        Uuid(id) => { out.push(12); out.extend_from_slice(id); }
        Bytes(bytes) => { out.push(14); frame(&mut out, bytes); }
        Decimal(scale, unscaled) => {
            out.push(8); out.extend_from_slice(&scale.to_be_bytes()); frame(&mut out, unscaled.as_bytes());
        }
        Vector(items) => {
            out.push(15); out.extend_from_slice(&(items.len() as u32).to_be_bytes());
            for item in items { out.extend_from_slice(&encode(item)); }
        }
        Map(items) => {
            let mut encoded: Vec<_> = items.iter().map(|(k, v)| (encode(k), encode(v))).collect();
            encoded.sort_by(|a, b| bytes_cmp(&a.0, &b.0));
            out.push(17); out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            for (k, v) in encoded { out.extend_from_slice(&k); out.extend_from_slice(&v); }
        }
    }
    out
}

struct Reader<'a> { data: &'a [u8], pos: usize }
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        assert!(n <= self.data.len() - self.pos, "truncated fixture");
        let data = &self.data[self.pos..self.pos+n]; self.pos += n; data
    }
    fn u32(&mut self) -> u32 { u32::from_be_bytes(self.take(4).try_into().unwrap()) }
    fn payload(&mut self) -> &'a [u8] { let n = self.u32() as usize; self.take(n) }
    fn text(&mut self) -> String { String::from_utf8(self.payload().to_vec()).unwrap() }
    fn node(&mut self, depth: usize) -> Node {
        assert!(depth <= 64);
        match self.take(1)[0] {
            3 => Long(i64::from_be_bytes(self.take(8).try_into().unwrap())),
            8 => { let scale = self.u32() as i32; Decimal(scale, self.text()) }
            9 => Text(self.text()),
            10 => Keyword(self.text(), self.text()),
            12 => Uuid(self.take(16).try_into().unwrap()),
            14 => Bytes(self.payload().to_vec()),
            15 => { let n = self.u32(); Vector((0..n).map(|_| self.node(depth+1)).collect()) }
            17 => {
                let n = self.u32();
                Map((0..n).map(|_| (self.node(depth+1), self.node(depth+1))).collect())
            }
            tag => panic!("unsupported fixture tag {tag}"),
        }
    }
}

fn uuid(s: &str) -> Node {
    let bytes = unhex(&s.replace('-', "")); Uuid(bytes.try_into().unwrap())
}
fn domain(epoch: i64, name: &str) -> Node {
    Vector(vec![uuid("11111111-1111-4111-8111-111111111111"), kw(name), Long(epoch)])
}
fn fixtures() -> BTreeMap<&'static str, (Node, Node)> {
    let tuple = map(vec![("kind", kw("tuple")),
                         ("elements", Vector(vec![kw("string"), kw("long")]))]);
    let inputs = vec![
        ("scalar", domain(0, "order"), vec![kw("long")], vec![Long(7)]),
        ("composite", domain(0, "order"), vec![kw("string"), kw("long")], vec![text("tenant-a"), Long(7)]),
        ("shared", kw("shared/order"), vec![kw("long")], vec![Long(7)]),
        ("epoch", domain(1, "order"), vec![kw("long")], vec![Long(7)]),
        ("version", domain(0, "order"), vec![kw("long")], vec![Long(7)]),
        ("bytes", kw("shared/order"), vec![kw("bytes")], vec![Bytes(vec![0, 1, 127, 255])]),
        ("export", domain(0, "export-row"), vec![kw("uuid"), tuple],
         vec![uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), Vector(vec![text("part-1"), Long(42)])]),
        ("decimal", kw("shared/order"), vec![kw("bigdec")], vec![Decimal(2, "100".into())]),
        ("extra", kw("shared/order"), vec![kw("long")], vec![Long(7)]),
    ];
    inputs.into_iter().map(|(id, domain, types, keys)| {
        let mut entries = vec![
            ("kind", kw("custom")), ("name", kw("datalevin.federation/entity-ref")),
            ("version", Long(if id == "version" { 2 } else { 1 })),
            ("fingerprint", text(if id == "version" { "fixture-entity-ref:wire-v2" } else { "fixture-entity-ref:wire-v1" })),
            ("parameters", map(vec![("identity-domain-id", domain), ("key-types", Vector(types))])),
        ];
        if id == "extra" {
            entries.push(("fixture/contract", map(vec![("meaning", text("alternate")), ("revision", Long(2))])));
        }
        (id, (map(entries), Vector(keys)))
    }).collect()
}
fn wire(t: &Node, k: &Node) -> Vec<u8> {
    let mut bytes = b"ERF\x01".to_vec();
    frame(&mut bytes, &encode(t)); frame(&mut bytes, &encode(k)); bytes
}
fn unhex(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i+2], 16).unwrap()).collect()
}
fn main() {
    let path = env::args().nth(1).expect("pass entity-ref-golden.tsv");
    let text = fs::read_to_string(path).unwrap();
    let mut expected = fixtures();
    let mut count = 0;
    for line in text.lines().filter(|s| !s.starts_with('#') && !s.is_empty()) {
        let (id, hex) = line.split_once('\t').expect("id<TAB>hex");
        let (t, k) = expected.remove(id).expect("duplicate or unknown fixture");
        let bytes = unhex(hex);
        assert_eq!(wire(&t, &k), bytes, "Rust encode differs: {id}");
        let mut r = Reader { data: &bytes, pos: 0 };
        assert_eq!(r.take(4), b"ERF\x01");
        for expected in [&t, &k] {
            let part = r.payload(); let mut inner = Reader { data: part, pos: 0 };
            let actual = inner.node(0); assert_eq!(inner.pos, part.len());
            assert_eq!(encode(&actual), encode(expected), "Rust decode differs: {id}");
        }
        assert_eq!(r.pos, bytes.len()); count += 1;
    }
    assert!(expected.is_empty(), "missing golden fixtures");
    println!("Verified {count} JVM/Rust entity-reference fixtures in both directions");
}
