# Folder (Web version)

This is a in-browser search engine that makes use of pre-generated index shards served by web server. It's
written in Rust and outputs a WebAssembly package. The API is still being improved.

## Installation

```
npm install --save folder-web-rs
```

## Usage

If you don't have the pre-generated index shards, then they will need to be created first by using
the [native binary](https://github.com/veeableful/folder). The [Rust version](https://github.com/veeableful/folder-rs) of the native binary is also being developed but it currently only implements searching so it can't create an index yet.

After they are generated, the index directory can be copied to where the web server can serve them. For example, if you have a web server listening on `http://localhost:8080`, the `index` directory can be copied to the web server root directory and be used by a web project like this:

```javascript
import * as wasm from "folder-web-rs"

let index = new wasm.IndexHandle('index', 'http://localhost:8080')
let result = await index.search('lunar new year')
```

## License

This project is licensed under the MIT License.
