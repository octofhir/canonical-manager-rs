#[cfg(feature = "fuzzy-search")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "fuzzy-search")]
#[derive(Debug, Default)]
pub struct NGramIndex {
    // trigram -> URLs containing it
    postings: HashMap<String, HashSet<String>>,
}

#[cfg(feature = "fuzzy-search")]
impl NGramIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
        }
    }

    pub fn build_from_urls<I: IntoIterator<Item = String>>(urls: I) -> Self {
        let mut idx = Self::new();
        for url in urls {
            idx.index(&url);
        }
        idx
    }

    pub fn index(&mut self, url: &str) {
        let lower = url.to_lowercase();
        for g in Self::trigrams(&lower) {
            self.postings.entry(g).or_default().insert(url.to_string());
        }
    }

    pub fn query(&self, query: &str, max_candidates: usize) -> Vec<(String, f64)> {
        let grams: HashSet<String> = Self::trigrams(&query.to_lowercase()).into_iter().collect();
        if grams.is_empty() {
            return vec![];
        }
        let mut counts: HashMap<String, usize> = HashMap::new();
        for g in &grams {
            if let Some(urls) = self.postings.get(g) {
                for u in urls {
                    *counts.entry(u.clone()).or_insert(0) += 1;
                }
            }
        }
        let mut scored: Vec<(String, f64)> = counts
            .into_keys()
            .map(|u| {
                let ugrams: HashSet<String> =
                    Self::trigrams(&u.to_lowercase()).into_iter().collect();
                let inter = grams.intersection(&ugrams).count() as f64;
                let union = grams.union(&ugrams).count() as f64;
                let jaccard = if union > 0.0 { inter / union } else { 0.0 };
                (u, jaccard)
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(max_candidates);
        scored
    }

    fn trigrams(s: &str) -> Vec<String> {
        let bytes = s.as_bytes();
        if bytes.len() < 3 {
            return vec![s.to_string()];
        }
        (0..=bytes.len() - 3)
            .map(|i| s[i..i + 3].to_string())
            .collect()
    }
}
