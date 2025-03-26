/*
 *     Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bytesize::ByteSize;
use criterion::{black_box, BenchmarkId, Criterion};
use dragonfly_client_storage::cache::lru_cache::LruCache;

// Number of operations to perform in each benchmark
const OPERATION_COUNT: usize = 1000;

pub fn lru_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Put");

    group.bench_with_input(
        BenchmarkId::new("Lru Put", "4MB"),
        &ByteSize::mb(4),
        |b, size| {
            b.iter_batched(
                || LruCache::new(OPERATION_COUNT),
                |mut cache| {
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Put", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || LruCache::new(OPERATION_COUNT),
                |mut cache| {
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Put", "16MB"),
        &ByteSize::mb(16),
        |b, size| {
            b.iter_batched(
                || LruCache::new(OPERATION_COUNT),
                |mut cache| {
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn lru_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Get");

    group.bench_with_input(
        BenchmarkId::new("Lru Get", "4MB"),
        &ByteSize::mb(4),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |mut cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.get(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Get", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |mut cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.get(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Get", "16MB"),
        &ByteSize::mb(16),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |mut cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.get(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn lru_peek(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Peek");

    group.bench_with_input(
        BenchmarkId::new("Lru Peek", "4MB"),
        &ByteSize::mb(4),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.peek(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Peek", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.peek(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Peek", "16MB"),
        &ByteSize::mb(16),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.peek(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn lru_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Contains");

    group.bench_with_input(
        BenchmarkId::new("Lru Contains", "4MB"),
        &ByteSize::mb(4),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.contains(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Contains", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.contains(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Contains", "16MB"),
        &ByteSize::mb(16),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |cache| {
                    for i in 0..OPERATION_COUNT {
                        black_box(cache.contains(&format!("key{}", i)));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

pub fn lru_pop_lru(c: &mut Criterion) {
    let mut group = c.benchmark_group("Lru Pop Lru");

    group.bench_with_input(
        BenchmarkId::new("Lru Pop Lru", "4MB"),
        &ByteSize::mb(4),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |mut cache| {
                    while !cache.is_empty() {
                        black_box(cache.pop_lru());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Pop Lru", "10MB"),
        &ByteSize::mb(10),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |mut cache| {
                    while !cache.is_empty() {
                        black_box(cache.pop_lru());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Lru Pop Lru", "16MB"),
        &ByteSize::mb(16),
        |b, size| {
            b.iter_batched(
                || {
                    let mut cache = LruCache::new(OPERATION_COUNT);
                    for i in 0..OPERATION_COUNT {
                        cache.put(format!("key{}", i), size.as_u64());
                    }
                    cache
                },
                |mut cache| {
                    while !cache.is_empty() {
                        black_box(cache.pop_lru());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}
