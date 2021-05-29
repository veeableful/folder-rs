use core::fmt::Debug;
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::cell::RefCell;

use csv::StringRecord;
use js_sys::Promise;
use json_dotpath::DotPaths;
use serde_json::{Value,Map};
use serde::{Serialize,Deserialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::{JsFuture, future_to_promise};
use web_sys::{Request, RequestInit, RequestMode, Response};

type TermStats = BTreeMap<Token, TermStat>;
type DocumentID = String;
type Token = String;
type ShardID = u32;

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Hit {
    pub id: String,
    pub score: f64,
    pub source: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
	pub count: usize,
	pub hits: Vec<Hit>,
}

#[wasm_bindgen]
pub struct SearchOptions {
	size: usize,     // Number of documents to return
	from: usize,     // Starting offset for returned documents
	use_cache: bool, // Store documents in memory
}

const DEFAULT_SEARCH_OPTIONS: SearchOptions = SearchOptions{
    size: 10,
    from: 0,
    use_cache: true,
};

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            size: 10,
            from: 0,
            use_cache: false,
        }
    }
}

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

#[wasm_bindgen]
pub struct Index {
    name: String,
    documents: BTreeMap<DocumentID, Value>,
    document_stats: BTreeMap<DocumentID, DocumentStat>,
    term_stats: TermStats,
    shard_count: usize,
    loaded_documents_shards: BTreeMap<usize, bool>,
    loaded_document_stats_shards: BTreeMap<usize, bool>,
    loaded_term_stats_shards: BTreeMap<usize, bool>,
    base_url: String,
}

#[wasm_bindgen]
pub struct IndexHandle {
    index: Rc<RefCell<Index>>,
}

#[wasm_bindgen]
impl IndexHandle {
    #[wasm_bindgen(constructor)]
    pub fn new(name: String, base_url: String) -> Self {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
        Self {
            index: Rc::new(RefCell::new(Index::new(name, base_url))),
        }
    }

    #[wasm_bindgen(method)]
    pub fn load(&self) -> Promise {
        let index = self.index.clone();
        future_to_promise(async move {
            Index::load(index).await?;
            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen(method)]
    pub fn search(&self, query: String) -> Promise {
        let index = self.index.clone();
        future_to_promise(async move {
            let result = Index::search(index, &query).await?;
            Ok(result)
        })
    }

    #[wasm_bindgen(method)]
    pub fn search_with_options(&self, query: String, opts: SearchOptions) -> Promise {
        let index = self.index.clone();
        future_to_promise(async move {
            let result = search_with_options(index, &query, opts).await?;
            Ok(JsValue::from_serde(&result).unwrap())
        })
    }
}

impl Index {
    fn new(name: String, base_url: String) -> Index {
        Index {
            name,
            documents: BTreeMap::new(),
            document_stats: BTreeMap::new(),
            term_stats: TermStats::new(),
            shard_count: 0,
            loaded_documents_shards: BTreeMap::new(),
            loaded_document_stats_shards: BTreeMap::new(),
            loaded_term_stats_shards: BTreeMap::new(),
            base_url,
        }
    }

    pub async fn load(this: Rc<RefCell<Index>>) -> Result<Rc<RefCell<Index>>, JsValue> {
        Index::load_shard_count(this.clone()).await?;
        Ok(this)
    }

    pub async fn search(this: Rc<RefCell<Index>>, query: &str) -> Result<JsValue, JsValue> {
        let result = search_with_options(this.clone(), query, DEFAULT_SEARCH_OPTIONS).await?;
        Ok(JsValue::from_serde(&result).unwrap())
    }

    async fn load_shard_count(this: Rc<RefCell<Index>>) -> Result<JsValue, JsValue> {
        let mut opts = RequestInit::new();
        opts.method("GET");
        opts.mode(RequestMode::Cors);
    
        let name = this.borrow().name.clone();
        let base_url = this.borrow().base_url.clone();
        let url = format!("{}/{}/{}", &base_url, &name, SHARD_COUNT_FILE_NAME);
        let request = Request::new_with_str_and_init(&url, &opts)?;
        let window = web_sys::window().unwrap();
        let response_value = JsFuture::from(window.fetch_with_request(&request)).await?;
        assert!(response_value.is_instance_of::<Response>());

        let response: Response = response_value.dyn_into().unwrap();
        Index::load_shard_count_from_response(this.clone(), response).await?;
        Ok(JsValue::NULL)
    }

    async fn load_shard_count_from_response(this: Rc<RefCell<Index>>, response: Response) -> Result<JsValue, JsValue> {
        let text = JsFuture::from(response.text().unwrap()).await?.as_string().unwrap();
        let n = text.parse::<usize>().unwrap();
        this.borrow_mut().shard_count = n;
        Ok(JsValue::NULL)
    }
}

fn analyze(s: &str) -> Vec<String> {
    s.split(&[',', '、', '　', ' '][..])
        .map(|s| s.to_lowercase())
        .map(|s| s.replace(PUNCTUATIONS, ""))
        .filter(|s| !STOP_WORDS.contains(&s.as_str()))
        .collect()
}

async fn search_with_options(this: Rc<RefCell<Index>>, query: &str, opts: SearchOptions) -> Result<SearchResult, JsValue> {
    if !opts.use_cache {
        let name = this.borrow().name.clone();
        let base_url = this.borrow().base_url.clone();
        let shard_count = this.borrow().shard_count;
        let mut index = Index::new(name, base_url);
        index.shard_count = shard_count;
        return do_search_with_options(Rc::new(RefCell::new(index)), query, opts).await
    }
    return do_search_with_options(this, query, opts).await
}

async fn do_search_with_options(this: Rc<RefCell<Index>>, query: &str, opts: SearchOptions) -> Result<SearchResult, JsValue> {
    let tmp = analyze(query);
    let tokens: Vec<&str> = tmp.iter().map(String::as_str).collect();
    for token in &tokens {
        let shard_id = calculate_shard_id(this.clone(), token);
        load_term_stats_from_shard(this.clone(), shard_id).await?;
    }
    let matched_document_ids = find_documents(this.clone(), &tokens)?;
    let (sorted_document_ids, scores ) = sort_documents(this.clone(), matched_document_ids, &tokens).await?;
    let count = sorted_document_ids.len();
    let hits = fetch_hits(this.clone(), sorted_document_ids, scores, opts.size, opts.from).await?;
    let result = SearchResult{ hits, count };
    Ok(result)
}

fn find_documents<'a>(this: Rc<RefCell<Index>>, tokens: &[&str]) -> Result<Vec<String>, JsValue> {
    let mut document_ids_set = HashSet::new();

    for token in tokens {
        let term_stat = if let Some(term_stat) = this.borrow().term_stats.get(*token) {
            term_stat.clone()
        } else {
            continue;
        };

        let ids: Vec<String> = term_stat.document_ids;
        if document_ids_set.len() == 0 {
            document_ids_set = ids.iter().cloned().collect();
        } else if document_ids_set.len() == 1 {
            break;
        } else {
            let mut new_ids_set = HashSet::new();
            for id in ids {
                if let Some(x) = document_ids_set.take(&id) {
                    new_ids_set.insert(x);
                }
            }
            document_ids_set = new_ids_set;
        }
    }

    let document_ids = document_ids_set.into_iter().collect();

    Ok(document_ids)
}

async fn sort_documents<'a>(this: Rc<RefCell<Index>>, document_ids: Vec<String>, tokens: &[&'a str]) -> Result<(Vec<String>, Vec<f64>), JsValue> {
    let mut document_id_scores = Vec::with_capacity(document_ids.len());

    for document_id in document_ids {
        let score = calculate_score(this.clone(), &document_id, tokens).await?;
        document_id_scores.push((document_id, score));
    }

    document_id_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    let (document_ids, scores) = document_id_scores.into_iter().unzip();
    Ok((document_ids, scores))
}

async fn fetch_hits(this: Rc<RefCell<Index>>, document_ids: Vec<String>, scores: Vec<f64>, size: usize, from: usize) -> Result<Vec<Hit>, JsValue> {
    let mut n = document_ids.len();
    let mut hits = Vec::new();

    if size == 0 || from >= n {
        return Ok(hits);
    }
    if n > size {
        n = size;
    }

    for (id, score) in document_ids.iter().cloned().zip(scores).skip(from).take(n) {
        let source = fetch_document(this.clone(), &id).await?;
        hits.push(Hit{ id: id.to_string(), score, source });
    }
    
    Ok(hits)
}

pub async fn fetch_document(this: Rc<RefCell<Index>>, document_id: &str) -> Result<Value, JsValue> {
    if this.borrow().shard_count == 0 {
        return Err(JsValue::FALSE);
    }

    let shard_id = calculate_shard_id(this.clone(), document_id);
    load_documents_from_shard(this.clone(), shard_id).await?;

    if let Some(document) = this.borrow_mut().documents.get(document_id) {
        Ok(document.clone())
    } else {
        return Err(JsValue::FALSE);
    }
}

async fn load_documents_from_shard(this: Rc<RefCell<Index>>, shard_id: ShardID) -> Result<(), JsValue> {
    if this.borrow().loaded_documents_shards.contains_key(&(shard_id as usize)) {
        return Ok(())
    }

    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let name = this.borrow().name.clone();
    let base_url = this.borrow().base_url.clone();
    let url = format!("{}/{}/{}/{}", &base_url, &name, &shard_id, DOCUMENTS_FILE_EXTENSION);
    let request = Request::new_with_str_and_init(&url, &opts)?;
    let window = web_sys::window().unwrap();
    let response_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    assert!(response_value.is_instance_of::<Response>());

    let response: Response = response_value.dyn_into().unwrap();
    load_documents_from_response(this.clone(), response).await?;
    this.borrow_mut().loaded_documents_shards.insert(shard_id as usize, true);

    Ok(())
}

async fn load_documents_from_response(this: Rc<RefCell<Index>>, response: Response) -> Result<(), JsValue> {
    let text = JsFuture::from(response.text().unwrap()).await?.as_string().unwrap();
    let mut csvr = csv::Reader::from_reader(text.as_bytes());

    let headers = csvr.headers().unwrap().clone();

    while let Some(result) = csvr.records().next() {
        let record = result.unwrap();
        let document_id = record.get(0).unwrap();
        let document = document_from_record(&headers, &record);
        this.borrow_mut().documents.insert(document_id.to_string(), document);
    }

    Ok(())
}

fn document_from_record(headers: &StringRecord, record: &StringRecord) -> Value {
    let mut document = Value::Object(Map::new());

    for (i, header) in headers.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        document.dot_set(header, &record[i]).unwrap();
    }

    document
}

fn calculate_shard_id(this: Rc<RefCell<Index>>, s: &str) -> ShardID {
    const Q: ShardID = 123456789;

    let mut result = 0;
    for c in s.chars() {
        result += Q + (c as u32) * (c as u32);
    }
    result *= Q;

    let shard_id = result % this.borrow().shard_count as u32;
    shard_id
}

async fn load_document_stats_from_shard(this: Rc<RefCell<Index>>, shard_id: ShardID) -> Result<(), JsValue> {
    if this.borrow().loaded_document_stats_shards.contains_key(&(shard_id as usize)) {
        return Ok(())
    }

    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let name = this.borrow().name.clone();
    let base_url = this.borrow().base_url.clone();
    let url = format!("{}/{}/{}/{}", &base_url, &name, &shard_id, DOCUMENT_STATS_FILE_EXTENSION);
    let request = Request::new_with_str_and_init(&url, &opts)?;
    let window = web_sys::window().unwrap();
    let response_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    assert!(response_value.is_instance_of::<Response>());

    let response: Response = response_value.dyn_into().unwrap();
    load_document_stats_from_response(this.clone(), response).await?;
    this.borrow_mut().loaded_document_stats_shards.insert(shard_id as usize, true);

    Ok(())
}

async fn load_document_stats_from_response(this: Rc<RefCell<Index>>, response: Response) -> Result<(), JsValue> {
    let text = JsFuture::from(response.text().unwrap()).await?.as_string().unwrap();
    let mut csvr = csv::Reader::from_reader(text.as_bytes());

    for result in csvr.records() {
        let record = result.unwrap();
        let document_id = record.get(0).unwrap();
        let tfs: Vec<&str> = record.get(1).unwrap().split(' ').collect();
        for v in tfs {
            let vv: Vec<&str> = v.split(':').collect();
            let term = vv.get(0).unwrap();
            let frequency = vv.get(1).unwrap();

            if !this.borrow().document_stats.contains_key(document_id) {
                this.borrow_mut().document_stats.insert(document_id.to_string(), DocumentStat::new());
            }

            if let Some(document_stat) = this.borrow_mut().document_stats.get_mut(document_id) {
                let frequency: usize = frequency.parse().unwrap();
                document_stat.get_term_frequency_mut().insert(term.to_string(), frequency);
            }
        }
    }

    Ok(())
}

async fn fetch_document_stat(this: Rc<RefCell<Index>>, document_id: &str) -> Result<Option<DocumentStat>, JsValue> {
    if !this.borrow().document_stats.contains_key(document_id) {
        let shard_id = calculate_shard_id(this.clone(), document_id);
        load_document_stats_from_shard(this.clone(), shard_id).await?;
    }
    if let Some(document_stat) = this.borrow().document_stats.get(document_id) {
        Ok(Some(document_stat.clone()))
    } else {
        Ok(None)
    }
}

pub async fn fetch_term_stat(this: Rc<RefCell<Index>>, token: &str) -> Result<Option<TermStat>, JsValue> {
    let term_stats = &this.borrow().term_stats;
    if term_stats.contains_key(token) {
        let term_stat = term_stats.get(token).unwrap();
        Ok(Some(term_stat.clone()))
    } else {
        let shard_id = calculate_shard_id(this.clone(), token);
        load_term_stats_from_shard(this.clone(), shard_id).await?;

        if let Some(term_stat) = this.borrow().term_stats.get(token) {
            return Ok(Some(term_stat.clone()));
        }

        Ok(None)
    }
}

async fn load_term_stats_from_shard(this: Rc<RefCell<Index>>, shard_id: ShardID) -> Result<(), JsValue> {
    if this.borrow().loaded_term_stats_shards.contains_key(&(shard_id as usize)) {
        return Ok(())
    }

    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let name = this.borrow().name.clone();
    let base_url = this.borrow().base_url.clone();
    let url = format!("{}/{}/{}/{}", &base_url, &name, &shard_id, TERM_STATS_FILE_EXTENSION);
    let request = Request::new_with_str_and_init(&url, &opts)?;
    let window = web_sys::window().unwrap();
    let response_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    assert!(response_value.is_instance_of::<Response>());

    let response: Response = response_value.dyn_into().unwrap();
    load_term_stats_from_response(this.clone(), response).await?;
    this.borrow_mut().loaded_documents_shards.insert(shard_id as usize, true);

    Ok(())
}

async fn load_term_stats_from_response(this: Rc<RefCell<Index>>, response: Response) -> Result<(), JsValue> {
    let text = JsFuture::from(response.text().unwrap()).await?.as_string().unwrap();
    let mut csvr = csv::Reader::from_reader(text.as_bytes());

    for result in csvr.records() {
        let record = result.unwrap();
        let term = &record[0];
        let document_ids = record[1].split(" ").map(String::from).collect();
        insert_term_stats_document_ids(this.clone(), term, document_ids);
    }

    Ok(())
}

fn insert_term_stats_document_ids(this: Rc<RefCell<Index>>, term: &str, document_ids: Vec<String>) {
    let mut index = this.borrow_mut();
    let mut term_stat = if let Some(term_stat) = index.term_stats.get_mut(term) {
        term_stat.clone()
    } else {
        TermStat::new()
    };
    term_stat.get_document_ids_mut().extend(document_ids);
    index.term_stats.insert(term.to_string(), term_stat.clone());
}

async fn calculate_score(this: Rc<RefCell<Index>>, document_id: &str, tokens: &[&str]) -> Result<f64, JsValue> {
    let mut score = 0.0;

    for token in tokens {
        let tf = term_frequency(this.clone(), document_id, token).await?;
        score += tf * inverse_document_frequency(this.clone(), token)?;
    }

    Ok(score)
}

async fn term_frequency(this: Rc<RefCell<Index>>, document_id: &str, token: &str) -> Result<f64, JsValue> {
    let document_stat = if let Some(document_stat) = fetch_document_stat(this, document_id).await? {
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

fn inverse_document_frequency(this: Rc<RefCell<Index>>, token: &str) -> Result<f64, JsValue> {
    let a = this.borrow().document_stats.len() as f64;
    let b = document_frequency(this.clone(), token);
    let frequency = a / b;
    Ok(frequency.log10())
}

fn document_frequency(this: Rc<RefCell<Index>>, token: &str) -> f64 {
    let n = this.borrow().term_stats[token].document_ids.len() as u32;
    n as f64
}