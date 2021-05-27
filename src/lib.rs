use core::fmt::Debug;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::time::{Duration, Instant};

use csv::StringRecord;
use serde_json::{Value,Map};
use serde::Serialize;

const PUNCTUATIONS: &[char]= &['!','"','#','$','%','&','(',')','*','+',',','-','.','/',':',';','<','=','>','?','@','[','\\',']','^','_','`','{','|','}','~'];
const STOP_WORDS: &[&str] = &[
	"a", "and", "are", "as", "at", "be", "but", "by", "for",
	"if", "in", "into", "is", "it", "no", "not", "of", "on",
	"or", "s", "such", "t", "that", "the", "their", "then",
	"there", "these", "they", "this", "to", "was", "will",
	"with", "www",
];

const DOCUMENTS_FILE_EXTENSION : &str = "dcs";
const DOCUMENT_STATS_FILE_EXTENSION : &str = "dst";
const TERM_STATS_FILE_EXTENSION : &str = "tst";
const SHARD_COUNT_FILE_NAME : &str = "shard_count";

#[derive(Default)]
pub struct Index {
    name: String,
    documents: BTreeMap<DocumentID, Value>,
    document_stats: BTreeMap<DocumentID, DocumentStat>,
    term_stats: TermStats,
    shard_count: usize,
    loaded_documents_shards: BTreeMap<usize, bool>,
    loaded_document_stats_shards: BTreeMap<usize, bool>,
    loaded_term_stats_shards: BTreeMap<usize, bool>,
}

type TermStats = BTreeMap<Token, TermStat>;
type TermStatsRef<'a> = BTreeMap<Token, TermStat>;

trait AnalyzableField {
    fn analyze(&self, parent_field_name: &str, m: &mut BTreeMap<String, Vec<String>>);
}

impl AnalyzableField for String {
    fn analyze(&self, parent_field_name: &str, m: &mut BTreeMap<String, Vec<String>>) {
        if let Some(entry) = m.get_mut(parent_field_name) {
            entry.push(self.clone());
        }
    }
}

impl AnalyzableField for Vec<String> {
    fn analyze(&self, parent_field_name: &str, m: &mut BTreeMap<String, Vec<String>>) {
        if let Some(entry) = m.get_mut(parent_field_name) {
            entry.extend(self.iter().cloned());
        }
    }
}

impl AnalyzableField for BTreeMap<String, Box<dyn AnalyzableField>> {
    fn analyze(&self, parent_field_name: &str, m: &mut BTreeMap<String, Vec<String>>) {
        for (field, value) in self.iter() {
            if parent_field_name.is_empty() {
                value.analyze(field, m);
            } else {
                let with_parent = format!("{}.{}", parent_field_name, field);
                value.analyze(&with_parent, m);
            };
            value.analyze(field, m);
        }
    }
}

impl AnalyzableField for Value {
    fn analyze(&self, parent_field_name: &str, m: &mut BTreeMap<String, Vec<String>>) {
        match &self {
            Value::String(value) => {
                value.analyze(parent_field_name, m);
            },
            Value::Array(value) => {
                if let Some(entry) = m.get_mut(parent_field_name) {
                    entry.extend(value.iter().filter_map(Value::as_str).map(String::from));
                }
            },
            Value::Object(value) => {
                for (field, value) in value.iter() {
                    if parent_field_name.is_empty() {
                        value.analyze(field, m);
                    } else {
                        let with_parent = format!("{}.{}", parent_field_name, field);
                        value.analyze(&with_parent, m);
                    }
                }
            },
            _ => {},
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Hit {
    id: String,
    score: f64,
    source: Value,
}

#[derive(Debug, Serialize)]
pub struct SearchTime {
    match_: Duration,
    sort: Duration,
    total: Duration,
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
	count: usize,
	hits: Vec<Hit>,
	time: SearchTime
}

impl SearchResult {
    pub fn len(&self) -> usize {
        return self.count;
    }
}

pub struct SearchOptions {
	size: usize,  // Number of documents to return
	from: usize,  // Starting offset for returned documents
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            size: 10,
            from: 0,
        }
    }
}

impl Index {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn load(index_name: &str) -> Result<Self, ()> {
        let mut index = Self::new();
        index.name = index_name.to_string();
        index.load_shard_count()?;
        Ok(index)
    }

    pub fn get_shard_count(&self) -> usize {
        self.shard_count
    }

    pub fn search(&mut self, query: &str) -> Result<SearchResult, ()> {
        search_with_options(query, SearchOptions::default(), &self.name, &mut self.documents, &mut self.loaded_documents_shards, &mut self.document_stats, &mut self.loaded_document_stats_shards, &mut self.term_stats, &mut self.loaded_term_stats_shards, self.shard_count)
    }

    /*
    fn index(&mut self, document_id: DocumentID, document: Value) -> Result<(), ()> {
        let mut m = BTreeMap::new();
        document.analyze(String::new(), &mut m);
        for (field, tokens) in m.iter() {
            self.index_tokens(&document_id, field.clone(), tokens.clone())?;
        }
        Ok(())
    }

    fn index_tokens(&mut self, document_id: &str, field: String, tokens: Vec<Token>) -> Result<(), ()> {
        self.update_document_stat(document_id, tokens.clone())?;
        self.update_term_stat(document_id, tokens.clone())?;
        if !self.field_names.contains(&field) {
            self.field_names.push(field);
        }
        Ok(())
    }
    */

    fn load_shard_count(&mut self) -> Result<(), ()> {
        let file_path = format!("{}/{}", &self.name, SHARD_COUNT_FILE_NAME);
        let file = File::open(&file_path).unwrap();
        self.load_shard_count_from_reader(file)?;
        Ok(())
    }

    fn load_shard_count_from_reader<T: Read>(&mut self, mut r: T) -> Result<(), ()> {
        let mut s = String::new();
        r.read_to_string(&mut s).unwrap();
        self.shard_count = s.parse::<usize>().unwrap() as usize;
        Ok(())
    }

    /*
    fn update_document_stat(&mut self, document_id: &str, tokens: Vec<Token>) -> Result<(), ()> {
        let mut document_stat = self.fetch_document_stat(document_id)?.unwrap().clone();

        for token in tokens {
            let term_frequency = document_stat.get_term_frequency_mut();
            let value = term_frequency.get(&token);
            if let Some(value) = value {
                term_frequency.insert(token, value + 1);
            } else {
                term_frequency.insert(token, 1);
            }
        }

        self.document_stats.insert(document_id.to_string(), document_stat.clone());

        Ok(())
    }

    fn update_term_stat(&mut self, document_id: &str, tokens: Vec<String>) -> Result<(), ()> {
        for token in tokens {
            let term_stat = self.fetch_term_stat(&token)?;
            let term_stat = match term_stat {
                Some(mut term_stat) => {
                    let document_ids = term_stat.get_document_ids_mut();
                    document_ids.push(document_id.to_string());
                    term_stat
                },
                None => {
                    let mut term_stat = TermStat::new();
                    term_stat.document_ids.push(document_id.to_string());
                    term_stat
                },
            };
            self.term_stats.insert(token, term_stat.clone());
        }

        Ok(())
    }
    */
}

fn analyze(s: &str) -> Vec<String> {
    s.split(&[',', '、', '　', ' '][..])
        .map(|s| s.to_lowercase())
        .map(|s| s.replace(PUNCTUATIONS, ""))
        .filter(|s| !STOP_WORDS.contains(&s.as_str()))
        .collect()
}

pub fn search_with_options(query: &str, opts: SearchOptions, index_name: &str, documents: &mut BTreeMap<DocumentID, Value>, loaded_documents_shards: &mut BTreeMap<usize, bool>, document_stats: &mut BTreeMap<DocumentID, DocumentStat>, loaded_document_stats_shards: &mut BTreeMap<usize, bool>, term_stats: &mut TermStats, loaded_term_stats_shards: &mut BTreeMap<usize, bool>, shard_count: usize) -> Result<SearchResult, ()> {
    let start_time = Instant::now();
    let tmp = analyze(query);
    let tokens: Vec<&str> = tmp.iter().map(String::as_str).collect();
    for token in &tokens {
        let shard_id = calculate_shard_id(token, shard_count);
        load_term_stats_from_shard(index_name, term_stats, loaded_term_stats_shards, shard_id)?;
    }
    let (matched_document_ids, match_duration) = find_documents(term_stats, &tokens)?;
    let (sorted_document_ids, scores, sort_duration) = sort_documents(&index_name, documents, document_stats, loaded_document_stats_shards, term_stats, shard_count, &matched_document_ids, &tokens)?;
    let hits = fetch_hits(index_name, documents, loaded_documents_shards, shard_count, &sorted_document_ids, scores, opts.size, opts.from)?;
    let count = sorted_document_ids.len();
    let total_duration = start_time.elapsed();
    Ok(SearchResult{
        count,
        hits,
        time: SearchTime{
            match_: match_duration,
            sort: sort_duration,
            total: total_duration,
        }
    })
}

fn find_documents<'a>(term_stats: &'a TermStats, tokens: &[&str]) -> Result<(Vec<&'a str>, Duration), ()> {
    let start_time = Instant::now();
    let mut document_ids_set = HashSet::new();

    for token in tokens {
        let term_stat = if let Some(term_stat) = term_stats.get(*token) {
            term_stat
        } else {
            continue;
        };

        let ids: Vec<&'a str> = term_stat.document_ids.iter().map(String::as_str).collect();
        if document_ids_set.len() == 0 {
            document_ids_set = ids.iter().cloned().collect();
        } else if document_ids_set.len() == 1 {
            break;
        } else {
            let mut new_ids_set = HashSet::new();
            for id in ids {
                if let Some(x) = document_ids_set.take(id) {
                    new_ids_set.insert(x);
                }
            }
            document_ids_set = new_ids_set;
        }
    }

    let document_ids = document_ids_set.into_iter().collect();
    let elapsed_time = start_time.elapsed();

    Ok((document_ids, elapsed_time))
}

pub fn sort_documents<'a>(index_name: &str, documents: &BTreeMap<DocumentID, Value>, document_stats: &mut BTreeMap<DocumentID, DocumentStat>, loaded_document_stats_shards: &mut BTreeMap<usize, bool>, term_stats: &'a TermStats, shard_count: usize, document_ids: &[&'a str], tokens: &[&'a str]) -> Result<(Vec<&'a str>, Vec<f64>, Duration), ()> {
    let start_time = Instant::now();
    let mut document_id_scores = Vec::with_capacity(document_ids.len());

    for document_id in document_ids {
        let score = calculate_score(index_name, documents, document_stats, loaded_document_stats_shards, term_stats, shard_count, &document_id, tokens)?;
        document_id_scores.push((document_id, score));
    }

    document_id_scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let (document_ids, scores) = document_id_scores.into_iter().unzip();
    Ok((document_ids, scores, start_time.elapsed()))
}

fn fetch_hits(index_name: &str, documents: &mut BTreeMap<DocumentID, Value>, loaded_documents_shards: &mut BTreeMap<usize, bool>, shard_count: usize, document_ids: &[&str], scores: Vec<f64>, size: usize, from: usize) -> Result<Vec<Hit>, ()> {
    let mut n = document_ids.len();
    let mut hits = Vec::new();

    if size == 0 || from >= n {
        return Ok(hits);
    }
    if n > size {
        n = size;
    }

    for (id, score) in document_ids.iter().cloned().zip(scores).skip(from).take(n) {
        let source = fetch_document(index_name, documents, loaded_documents_shards, &id, shard_count)?;
        hits.push(Hit{ id: id.to_string(), score, source });
    }
    
    Ok(hits)
}

pub fn fetch_document(index_name: &str, documents: &mut BTreeMap<DocumentID, Value>, loaded_documents_shards: &mut BTreeMap<usize, bool>, document_id: &str, shard_count: usize) -> Result<Value, ()> {
    if shard_count == 0 {
        return Err(());
    }

    let shard_id = calculate_shard_id(document_id, shard_count);
    load_documents_from_shard(index_name, documents, loaded_documents_shards, shard_id)?;

    if let Some(document) = documents.get(document_id) {
        Ok(document.clone())
    } else {
        Err(())
    }
}

fn load_documents_from_shard(index_name: &str, documents: &mut BTreeMap<DocumentID, Value>, loaded_documents_shards: &mut BTreeMap<usize, bool>, shard_id: ShardID) -> Result<(), ()> {
    if loaded_documents_shards.contains_key(&shard_id) {
        return Ok(())
    }

    let file_path = format!("{}/{}/{}", index_name, &shard_id, DOCUMENTS_FILE_EXTENSION);
    let file = File::open(&file_path).unwrap();
    load_documents_from_reader(documents, file)?;
    loaded_documents_shards.insert(shard_id, true);

    Ok(())
}

fn load_documents_from_reader<T: Read>(documents: &mut BTreeMap<DocumentID, Value>, r: T) -> Result<(), ()> {
    let mut csvr = csv::Reader::from_reader(r);
    let mut headers = StringRecord::new();
    let mut i = 0;

    for result in csvr.records() {
        if i == 0 {
            headers = result.unwrap();
            i += 1;
            continue;
        }
        let record = result.unwrap();
        let document_id = record.get(0).unwrap();
        let document = document_from_record(&headers, &record);
        documents.insert(document_id.to_string(), document);

        i += 1;
    }

    Ok(())
}

fn calculate_shard_id(s: &str, shard_count: usize) -> ShardID {
    const Q: ShardID = 123456789;

    let mut result = 0;
    for c in s.chars() {
        result += Q + (c as usize) * (c as usize);
    }
    result *= Q;

    let shard_id = result % shard_count;
    shard_id
}

fn load_document_stats_from_shard(index_name: &str, document_stats: &mut BTreeMap<DocumentID, DocumentStat>, loaded_document_stats_shards: &mut BTreeMap<usize, bool>, shard_id: ShardID) -> Result<(), ()> {
    if loaded_document_stats_shards.contains_key(&shard_id) {
        return Ok(())
    }

    let file_path = format!("{}/{}/{}", index_name, &shard_id, DOCUMENT_STATS_FILE_EXTENSION);
    let file = File::open(&file_path).unwrap();
    load_document_stats_from_reader(document_stats, file)?;
    loaded_document_stats_shards.insert(shard_id, true);

    Ok(())
}

fn load_document_stats_from_reader<T: Read>(document_stats: &mut BTreeMap<DocumentID, DocumentStat>, r: T) -> Result<(), ()> {
    let mut csvr = csv::Reader::from_reader(r);

    for result in csvr.records() {
        let record = result.unwrap();
        let document_id = record.get(0).unwrap();
        let tfs: Vec<&str> = record.get(1).unwrap().split(' ').collect();
        for v in tfs {
            let vv: Vec<&str> = v.split(':').collect();
            let term = vv.get(0).unwrap();
            let frequency = vv.get(1).unwrap();

            if !document_stats.contains_key(document_id) {
                document_stats.insert(document_id.to_string(), DocumentStat::new());
            }

            if let Some(document_stat) = document_stats.get_mut(document_id) {
                let frequency: usize = frequency.parse().unwrap();
                document_stat.get_term_frequency_mut().insert(term.to_string(), frequency);
            }
        }
    }

    Ok(())
}

fn fetch_document_stat<'a>(index_name: &str, document_stats: &'a mut BTreeMap<DocumentID, DocumentStat>, loaded_document_stats_shards: &mut BTreeMap<usize, bool>, document_id: &str, shard_count: usize) -> Result<Option<&'a DocumentStat>, ()> {
    if !document_stats.contains_key(document_id) {
        let shard_id = calculate_shard_id(&document_id, shard_count);
        load_document_stats_from_shard(index_name, document_stats, loaded_document_stats_shards, shard_id)?;
    }
    Ok(document_stats.get(document_id))
}

pub fn fetch_term_stat<'a>(index_name: &str, term_stats: &'a mut TermStats, loaded_term_stats_shards: &mut BTreeMap<usize, bool>, token: &str, shard_count: usize) -> Result<Option<&'a TermStat>, ()> {
    if term_stats.contains_key(token) {
        let term_stat = term_stats.get(token).unwrap();
        Ok(Some(term_stat))
    } else {
        let shard_id = calculate_shard_id(token, shard_count);
        load_term_stats_from_shard(index_name, term_stats, loaded_term_stats_shards, shard_id)?;

        if let Some(term_stat) = term_stats.get(token) {
            return Ok(Some(term_stat));
        }

        Ok(None)
    }
}

fn load_term_stats_from_shard(index_name: &str, term_stats: &mut TermStatsRef, loaded_term_stats_shards: &mut BTreeMap<usize, bool>, shard_id: ShardID) -> Result<(), ()> {
    if loaded_term_stats_shards.contains_key(&shard_id) {
        return Ok(())
    }

    let file_path = format!("{}/{}/{}", index_name, &shard_id, TERM_STATS_FILE_EXTENSION);
    let file = File::open(&file_path).unwrap();
    load_term_stats_from_reader(term_stats, file)?;

    loaded_term_stats_shards.insert(shard_id, true);

    Ok(())
}

fn load_term_stats_from_reader<T: Read>(term_stats: &mut TermStatsRef, r: T) -> Result<(), ()> {
    let mut csvr = csv::Reader::from_reader(r);

    for result in csvr.records() {
        let record = result.unwrap();
        let term = &record[0];
        let document_ids = record[1].split(" ").map(String::from).collect();
        insert_term_stats_document_ids(term_stats, term, document_ids);
    }

    Ok(())
}

fn insert_term_stats_document_ids(term_stats: &mut TermStatsRef, term: &str, document_ids: Vec<String>) {
    let mut term_stat = if let Some(term_stat) = term_stats.get_mut(term) {
        term_stat.clone()
    } else {
        TermStat::new()
    };
    term_stat.get_document_ids_mut().extend(document_ids);
    term_stats.insert(term.to_string(), term_stat.clone());
}

fn calculate_score(index_name: &str, documents: &BTreeMap<DocumentID, Value>, document_stats: &mut BTreeMap<DocumentID, DocumentStat>, loaded_document_stats_shards: &mut BTreeMap<usize, bool>, term_stats: &TermStatsRef, shard_count: usize, document_id: &str, tokens: &[&str]) -> Result<f64, ()> {
    let mut score = 0.0;

    for token in tokens {
        let tf = term_frequency(index_name, document_stats, loaded_document_stats_shards, document_id, token, shard_count)?;
        score += tf * inverse_document_frequency(documents, term_stats, token)?;
    }

    Ok(score)
}

fn term_frequency(index_name: &str, document_stats: &mut BTreeMap<DocumentID, DocumentStat>, loaded_document_stats_shards: &mut BTreeMap<usize, bool>, document_id: &str, token: &str, shard_count: usize) -> Result<f64, ()> {
    let document_stat = if let Some(document_stat) = fetch_document_stat(index_name, document_stats, loaded_document_stats_shards, document_id, shard_count)? {
        document_stat
    } else {
        return Ok(0.0);
    };

    let term_frequency = document_stat
        .term_frequency
        .get(token)
        .map_or(0.0, |n| *n as f64);
    Ok(term_frequency)
}

fn inverse_document_frequency(documents: &BTreeMap<DocumentID, Value>, term_stats: &TermStatsRef, token: &str) -> Result<f64, ()> {
    let frequency = (documents.len() as f64) / document_frequency(term_stats, token);
    Ok(frequency.log10())
}

fn document_frequency(term_stats: &TermStatsRef, token: &str) -> f64 {
    term_stats[token].document_ids.len() as f64
}

type DocumentID = String;
type Token = String;
type ShardID = usize;

#[derive(Clone)]
pub struct DocumentStat {
    term_frequency: BTreeMap<String, usize>,
}

impl DocumentStat {
    pub fn new() -> DocumentStat {
        DocumentStat {
            term_frequency: BTreeMap::new(),
        }
    }

    pub fn get_term_frequency_mut(&mut self) -> &mut BTreeMap<String, usize> {
        &mut self.term_frequency
    }
}

#[derive(Clone)]
pub struct TermStat {
    document_ids: Vec<String>,
}

impl TermStat {
    pub fn new() -> TermStat {
        TermStat {
            document_ids: Vec::new(),
        }
    }

    pub fn get_document_ids(&self) -> &Vec<String> {
        &self.document_ids
    }

    pub fn get_document_ids_mut(&mut self) -> &mut Vec<String> {
        &mut self.document_ids
    }
}

fn document_from_record(headers: &StringRecord, record: &StringRecord) -> Value {
    let mut document = Value::Object(Map::new());

    for (i, header) in headers.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        set_field(&mut document, header, &record[i]);
    }

    document
}

fn set_field(document: &mut Value, header: &str, value: &str) {
    let fields: Vec<&str> = header.split(".").collect();
    let mut it: Value = document.clone();
	let mut level = fields.len();

    for field in fields {
        level -= 1;

        if let Value::Object(m) = &mut it {
            if !m.contains_key(field) {
                continue
            }

            if level > 0 {
                m.insert(field.to_string(), Value::Object(Map::new()));
            } else {
                m.insert(field.to_string(), Value::String(value.to_string()));
            }
        }
    }
}