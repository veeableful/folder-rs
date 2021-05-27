const PUNCTUATIONS: &[char]= &['!','"','#','$','%','&','(',')','*','+',',','-','.','/',':',';','<','=','>','?','@','[','\\',']','^','_','`','{','|','}','~'];
const STOP_WORDS: &[&str] = &[
	"a", "and", "are", "as", "at", "be", "but", "by", "for",
	"if", "in", "into", "is", "it", "no", "not", "of", "on",
	"or", "s", "such", "t", "that", "the", "their", "then",
	"there", "these", "they", "this", "to", "was", "will",
	"with", "www",
];

// lowercase_filter converts the tokens into their lowercase counterparts
pub fn lowercase_filter(tokens: Vec<String>) -> Vec<String> {
    let mut result = Vec::new();

    for token in tokens {
        result.push(token.to_lowercase());
    }

	result
}

// punctuation_filter removes punctuations from tokens
pub fn punctuation_filter(tokens: Vec<String>) -> Vec<String> {
    let mut result = Vec::new();

    for token in tokens {
        result.push(token.replace(PUNCTUATIONS, ""));
    }

	result
}

// stop_word_filter removes tokens that are stop words
pub fn stop_word_filter(tokens: Vec<String>) -> Vec<String> {
    let mut result = Vec::new();

    for token in tokens {
		if !STOP_WORDS.contains(&token.as_str()) {
			result.push(token.replace(PUNCTUATIONS, ""));
		}
    }

	result
}