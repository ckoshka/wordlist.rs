//use mimalloc::MiMalloc;

//#[global_allocator]
//static GLOBAL: MiMalloc = MiMalloc;
use dashmap::DashMap;
use fnv::FnvHasher;
use nohash_hasher::IntSet;
use rayon::prelude::*;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Write;
use term_macros::*;
use unicode_segmentation::UnicodeSegmentation;
use std::collections::HashMap;

type WordId = u32;

#[derive(Debug)]
pub struct Context {
    concepts: IntSet<WordId>,
    all_concepts: usize,
    skip: bool
}

impl Context {
    pub fn from_set(concepts: IntSet<WordId>) -> Self {
        Context {
            all_concepts: concepts.len(),
            concepts,
            skip: false
        }
    }
}

#[derive(Debug)]
pub struct ContextHolder {
    ctxs: Vec<Context>,
}

pub fn fill_map<'a>(
    iter: impl IntoParallelIterator<Item = &'a Context> + Send + Sync,
    preallocated_map: &DashMap<WordId, usize>,
) {
    preallocated_map.clear();
    iter.into_par_iter().for_each(|ctx| {
        ctx.concepts.iter().for_each(|u| {
            let mut count = preallocated_map.entry(*u).or_insert(0);
            *count += ctx.all_concepts;
        });
    });
}

pub fn sort_by_freq(items: &mut Vec<WordId>, map: &DashMap<WordId, usize>) {
    items.par_sort_by_cached_key(|u| map.get(u).map(|v| *v).unwrap_or_else(|| 0));
}

pub fn strip_known(
    holder: &mut ContextHolder,
    knowns: &IntSet<WordId>,
    preallocated_map: &DashMap<WordId, usize>,
    desired_words: &mut HashMap<WordId, usize>
) -> Vec<WordId> {
    preallocated_map.clear();

    let n1s: IntSet<WordId> = holder
        .ctxs
        .par_iter_mut()
        .filter(|c| c.skip == false)
        .map(|c| {
            let intsect = c.concepts.intersection(&knowns).next();
            if intsect.is_some() {
                c.concepts.retain(|word| !knowns.contains(word));
                if c.concepts.len() == 0 {
                    c.skip = true;
                    return None;
                }
            };
            if c.concepts.len() == 1 {
                return Some(*c.concepts.iter().next().unwrap());
            } else if c.concepts.len() == 2 {
                c.concepts.iter().for_each(|u| {
                    let mut count = preallocated_map.entry(*u).or_insert(0);
                    *count += 1
                });
            }
            return None;
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect();

    let mut n_1: Vec<_> = n1s.into_iter().collect();
    if n_1.len() == 0 {
        panic!("Sentences weren't viable");
    }

    if desired_words.len() == 0 {
        sort_by_freq(&mut n_1, &preallocated_map);
    } else {
        n_1.sort_by_cached_key(|u| desired_words.get(u)
            .map(|v| *v * 20000)
            .unwrap_or_else(|| preallocated_map.get(u).map(|v| *v).unwrap_or_else(|| 0))
        );
    }
    n_1
}

pub fn yield_concepts<'a>(
    mut holder: ContextHolder,
    id_to_word: std::collections::HashMap<WordId, String>,
    desired_words: &mut HashMap<WordId, usize>
) {
    let freq_map = DashMap::<WordId, usize>::with_capacity(500_000);
    let wtr = std::io::stdout();
    let mut lock = wtr.lock();
    let mut new_concepts = IntSet::<WordId>::default();
    let mut total = 26;
    loop {
        new_concepts = strip_known(&mut holder, &new_concepts, &freq_map, desired_words).into_iter().rev().take(total / 2).collect();
        for id in new_concepts.iter() {
            id_to_word.get(&id).map(|val| {
                total += 1;
                let _ = lock.write_all(val.as_bytes());
                lock.write_all(b" ").unwrap();
            });
        }
        lock.write_all(b"\n").unwrap();
    }
}

fn hash_str(s: &str) -> WordId {
    let mut h = FnvHasher::with_key(0);
    s.hash(&mut h);
    h.finish() as u32
}

pub fn main() {
    tool! {
        args:
            - existing: String = String::new();
            - desired: String = String::new();
            - ctxs_msgpack: Option<String> = None;
            - dict_msgpack: Option<String> = None;
        ;
        body: || {
            let already_known_words = existing.split(" ").collect::<HashSet<_>>();
            let _desired_words = desired.split(" ").collect::<Vec<_>>();
            let mut desired_words = _desired_words.into_iter().rev().enumerate().map(|(i, x)| (hash_str(x), i)).collect::<HashMap<_, _>>();

            let mut holder = ContextHolder { ctxs: Vec::with_capacity(1_000_000) };
            let mut mappings = std::collections::HashMap::with_capacity(300_000);

            if ctxs_msgpack.is_some() {
                let ctxs: Vec<IntSet<WordId>> = ctxs_msgpack.map(|s| mmap!(s)).map(|m| rmp_serde::from_slice(&m).unwrap()).unwrap();
                let dict: HashMap<String, WordId> = dict_msgpack.map(|s| mmap!(s)).map(|m| rmp_serde::from_slice(&m).unwrap()).expect("if the ctxs msgpack is specified, so too must be the dict");
                let dict: HashMap<WordId, String> = dict.into_iter().map(|(word, id)| (id, word)).collect();
                mappings = dict;
                holder.ctxs.extend(ctxs.into_iter().map(|i| Context::from_set(i)));

            } else {

                readin!(_wtr, |byteline: &[u8]| {
                    let _ = std::str::from_utf8(byteline).map(|line| {
                        holder.ctxs.push(Context::from_set(line
                                .unicode_words()
                                .map(|w| w.to_lowercase())
                                .filter(|w| !already_known_words.contains(w.as_str()))
                                    .filter(|w| w.len() > 0)
                                    .map(|w| {
                                        let id = hash_str(&w);
                                        if mappings.get(&id).is_none() {
                                            mappings.insert(id, w);
                                        };
                                        id
                                    }).collect()
                        ));
                    });
                });

            }

            yield_concepts(holder, mappings, &mut desired_words);
        }
    }
}
