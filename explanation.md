# History Analysis: Naive vs Optimized

## 1. Files included in the project

The history artifacts are now organized in:

- `analyse_history/naive/`
- `analyse_history/optimized/`

## 2. What the application computes

The project analyzes Reddit comments and computes, for each `(year, subreddit)`, the top `N` words with:

- `comments`: number of valid comments
- `avg_score`: average Reddit score
- `word`: retained word
- `occurrences`: number of times the word appears
- `total_tokens`: total retained tokens in that `(year, subreddit)`
- `relative_frequency`: `occurrences / total_tokens`
- `rank`: ranking of the word in that subgroup

This means the result tells us:
- which subreddits were active in each year
- which words dominated discussion
- how important those words were relative to the full vocabulary of that subgroup

## 3. Reminder of the two implementations

### Naive version

Entrypoint:
- `job.FirstAnalysis`

#### Key idea

The dataset is processed multiple times independently.
It creates 3 full RDD pipelines:

- word counts
- token totals
- comment statistics

#### Problem

Each branch triggers:
- full scan of input dataset (~8.5 GB)
- independent transformations

### Optimized version

Entrypoint:
- `job.SecondAnalysis`

#### Key idea
The dataset is:

- loaded once
- normalized once
- reused via persist()

`persist(StorageLevel.MEMORY_AND_DISK)
repartition(200)`

#### Improvement
- avoids repeated CSV scans
- reduces shuffle pressure
- improves executor reuse

## 4. Shuffle behavior

Both versions include multiple shuffle stages:

#### Shuffles come from:
- reduceByKey
- groupByKey
- join
- ranking per group
- repartitioning (optimized version)

## 5. Execution plan analysis

### Common pipeline steps

Both versions include:

- FileScan csv
- Filter
- Generate (explode tokens)
- HashAggregate
- Exchange (shuffle)
- Join
- Window (ranking)
- WriteFiles

## 6. Naive execution analysis

#### Observations from Spark UI
- multiple full dataset scans (~7.9 GB repeated)
- repeated map / flatMap branches
- higher shuffle duplication

#### Key stages (naive)
- Stage ~0–2:
  - full dataset scans
  - tokenization + aggregation
- Stage ~4: 
  - heavy shuffle (~3.8 GB)
- Final stages:
  - ranking + write

#### Problem summary

The dataset is effectively processed 3 times independently

## 7.Optimized execution analysis

#### Key improvements visible in Spark UI
- repartition(200) creates controlled parallelism
- persist() avoids recomputation
- fewer full scans
- 
## 8. Stage interpretation (optimized)

### Stage 0 (main heavy stage)
- input: ~7.9 GB 
- shuffle write: large (~4–5 GB range)

Word-level aggregation `(year, subreddit, word)`

## Intermediate stages
- reduced shuffle sizes (hundreds of MB)
- joins between aggregated datasets
- broadcast joins used when possible

## Final stage
- output: ~30–40 MB only
- coalesced write (coalesce(1))

## 9. Why output is small

#### Input:
- 8.5 GB raw comments
#### Output:
- only aggregated top-N words

#### Massive reduction due to:
- grouping
- filtering stopwords
- ranking

## 10. Naive vs Optimized comparison

| Aspect       | Naive      | Optimized        |
| ------------ | ---------- | ---------------- |
| Input scans  | 3 full scans | 1 logical pipeline |
| RDD reuse    |  No        |  Yes             |
| Persistence  |  No        |  Yes             |
| Shuffle size | Higher     | Lower            |
| Runtime      | Slower     | Faster           |
| Stability    | Lower      | Higher           |

## 11. Key Spark optimizations used
- persist(MEMORY_AND_DISK)
- repartition(200)
- reduceByKey instead of heavy shuffles
- reuse of RDDs in multiple branches
- controlled join strategy