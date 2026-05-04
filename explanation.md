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

Main idea:
- recompute each analytical branch starting from the raw CSV

So the naive version separately recomputes:
- `wordCounts`
- `tokenTotals`
- `commentStats`

### Optimized version

Entrypoint:
- `job.SecondAnalysis`

Main idea:
- normalize and repartition intermediate data once
- reuse those intermediates instead of rebuilding everything repeatedly

The optimized version persists intermediate data and reduces redundant work.

## 4. Why the job contains several shuffles

The project requirement asks for at least two shuffles.

This job clearly satisfies that because it includes:

- aggregation by `(year, subreddit, word)`
- aggregation by `(year, subreddit)` for token totals
- aggregation by `(year, subreddit)` for comment stats
- joins between aggregated datasets
- final ordering and window ranking

## 5. How to read the main Spark operators

The SQL DAG contains the following important operators:

### `FileScan csv`
Spark reads the input CSV file.

### `Filter`
Spark removes invalid rows:
- empty subreddit
- empty body
- deleted or removed comments
- null year
- null score

### `Project`
Spark creates transformed columns, such as:
- trimmed subreddit
- cleaned tokens
- token counts

### `Generate explode(tokens)`
One comment becomes many word rows.

This greatly increases the number of rows before aggregation, which is why the word-count branch is expensive.

### `HashAggregate`
Spark groups rows and computes:
- word counts
- token totals
- comment counts
- average score

### `Exchange`
Spark redistributes data between executors.

This is a shuffle.

### `SortMergeJoin` / `BroadcastHashJoin`
Spark joins the aggregated datasets.

### `Window` and `WindowGroupLimit`
Spark computes the top `N` words inside each `(year, subreddit)` partition.

### `WriteFiles`
Spark writes the final CSV.

## 7. The Input, Shuffle Read, Shuffle Write, Output values meaning


### `Input`

The stage shows `Input = 7.9 GiB`, we almost read the full dataset.

### `Shuffle Write`
This is the amount of intermediate data written so that other executors can read it later.
It's after operations such as:
- `groupBy`
- `join`
- `sort`
- `window`

### `Shuffle Read`
This is the shuffled data consumed by a later stage.

### `Output`
It's the amount written as final stage output.

In this project, the final output is much smaller than the raw input because:
- the input is billions of characters / many comments
- the output only keeps aggregated top words per `(year, subreddit)`

That is why the final stage writes only a few tens of MiB.

## 8. Optimized version: screenshot-by-screenshot analysis

### A. `analyse_history/optimized/optimized_jobs.png`

We can see:

- Job `0`: `8.5 min`
- Job `1`: `13 min`
- Job `2`: `4.4 min`
- Job `3`: `6.1 min`
- several other jobs are very short: `4 s`, `0.2 s`, `46 s`, `38 s`, `3 s`

Interpretation:

- the optimized run still contains multiple jobs because the final write, shuffles and adaptive execution trigger several phases
- the long jobs are the real computational core
- the short jobs at the end correspond to lighter finalization, small shuffle stages, final ranking or final writing

The presence of many skipped tasks and skipped stages are normal because of Spark SQL and Adaptive Query Execution.
Spark optimized or replaced parts of the initial plan.

### B. `analyse_history/optimized/optimized_stages.png`

We see :

#### Stage 0
- duration: `8.5 min`
- tasks: `64`
- input: `7.9 GiB`
- shuffle write: `4.9 GiB`

Interpretation:
- this is the heaviest stage of the optimized job
- it reads the full dataset
- it performs the expensive word-level branch
- it produces the biggest intermediate shuffle

The shuffle writes `4.9 GiB` because this step is after:
- filtering
- tokenization
- exploding comments into words

Spark must redistribute a very large word-level intermediate dataset for aggregation by:
- `(year, subreddit, word)`

That intermediate word-level representation is much less than the original input but way larger than the final output.

#### Stage 1
- duration: `5.0 min`
- tasks: `64`
- input: `7.9 GiB`
- shuffle write: `425.0 MiB`

Interpretation:
- this is another full input stage
- it is much lighter than stage 0
- it corresponds to a branch with far less cardinality than the full word-count branch

It writes only `425 MiB` because:
- it does not produce as much exploded data as the main word-count branch
- its aggregation keys are coarser

#### Stage 3
- duration: `24 s`
- tasks: `11`
- shuffle read: `4.9 GiB`
- shuffle write: `2.3 MiB`

Interpretation:
- this stage consumes the large shuffle generated by stage 0 then it reduces it aggressively

This means:
- stage 0 creates a huge intermediate representation
- stage 3 collapses it into much smaller aggregated results

#### Stage 4
- duration: `1.9 min`
- tasks: `11`
- shuffle read: `4.9 GiB`
- shuffle write: `1031.6 MiB`

Interpretation:
- this stage also consumes the huge word-level shuffle
- it performs another expensive aggregation/join preparation phase

#### Stage 8
- duration: `4 s`
- tasks: `9`
- shuffle read: `425.0 MiB`
- shuffle write: `2.6 MiB`

Interpretation:
- this is a much smaller branch
- it reduces a medium-size shuffle ( 425.0 MiB shows that he read the shuffle of stage 1 ) to a very small result

#### Stage 14
- duration: `46 s`
- tasks: `70`
- shuffle read: `1031.6 MiB`

#### Stage 17
- duration: `38 s`
- tasks: `70`
- shuffle read: `1031.6 MiB`
- shuffle write: `25.6 MiB`

Interpretation:
- these stages belong to the final join/ranking section
- the dataset size is already much smaller than in the early stages
- Spark is preparing the final ranked result

#### Stage 21
- duration: `3 s`
- tasks: `1`
- output: `38.6 MiB`
- shuffle read: `25.6 MiB`

Interpretation:
- this is the final output stage
- only one task writes because the result is coalesced/repartitioned into one final CSV part

The output is only `38.6 MiB` because the raw data is heavily reduced by aggregation:
- from billions of characters and millions of tokens
- to top `N` words per `(year, subreddit)`

### C. `analyse_history/optimized/optimized_timeline.png`

This screenshot is the event timeline for the optimized run.

Important observations:

- `driver`, `executor 1`, and `executor 2` are added at the beginning
- `executor 3` is added later
- `executor 1` is removed later
- jobs overlap in time

Interpretation:

- Spark is using dynamic resource behavior during the run
- the optimized application adapts its execution over time
- the cluster is not static during the full run

## 9. Optimized SQL DAG interpretation

From `analyse_history/optimized/reddit-analysis-optimized - Details for Query 0.html`, the key operators are:

- `FileScan csv`
- `RepartitionByExpression [year]`
- `Generate explode(tokens)`
- `HashAggregate`
- `BroadcastHashJoin`
- `AQEShuffleRead coalesced`
- `ReusedExchange`
- `Window`
- `WriteFiles`

### Main operations

#### `RepartitionByExpression [year]`
The optimized version explicitly repartitions intermediate data by `year`.

This helps:
- distribute work more predictably
- prepare later aggregations, the transformation is faster

#### `BroadcastHashJoin`
Spark decides that some aggregated datasets are small enough to broadcast and dont need a heavy join shuffle and reduces the cost of final joins.

#### `AQEShuffleRead coalesced`
Spark improves the handle of partition at runtime.

#### `ReusedExchange`
Spark is reusing result instead of recomputing it which let us win a lot of time again

## 10. Naive version: screenshot-by-screenshot analysis

### A. `analyse_history/naive/naive_jobs.png`

We see again several jobs for the app but the times are higher
- Job `0`: `8.5 min`
- Job `1`: `16 min`
- Job `2`: `20 min`
- Job `3`: `12 min`
- final short jobs: `49 s`, `45 s`, `3 s`

Interpretation:
- the central jobs are significantly longer than in the optimized version

### B. `analyse_history/naive/naive_stages.png`

We see again

#### Stage 0
- duration: `8.5 min`
- tasks: `64`
- input: `7.9 GiB`
- shuffle write: `3.8 GiB`

Interpretation:
- first full scan of the dataset
- expensive word-count branch

#### Stage 1
- duration: `8.2 min`
- tasks: `64`
- input: `7.9 GiB`
- shuffle write: `13.8 MiB`

Interpretation:
- second full scan of the dataset
- another branch with much lower output cardinality

#### Stage 2
- duration: `4.6 min`
- tasks: `64`
- input: `7.9 GiB`
- shuffle write: `15.7 MiB`

Interpretation:
- third full scan of the dataset

**the raw dataset is read three times**

It's because the naive application do:
- one branch for `wordCounts`
- one branch for `tokenTotals`
- one branch for `commentStats`
- 
That's the main reason why this version is not optimized

#### Stage 4
- duration: `32 s`
- tasks: `200`
- shuffle read: `3.8 GiB`
- shuffle write: `1454.6 MiB`

Interpretation:
- stage 4 consumes the large shuffle produced by stage 0
- it corresponds to a post-shuffle aggregation/join preparation phase

#### Other stages

The other stages are the final output one to write the result

### C. `analyse_history/naive/naive-timeline.png`

We can see :

- the naive job keeps the executors busy for longer
- most of the cost comes from repeated heavy branches rather than from a more adaptive execution

## 11. Naive SQL DAG interpretation

From `analyse_history/naive/reddit-analysis-naive - Details for Query 0.html`, the most important operators are:

- `FileScan csv`
- `Generate explode(tokens)`
- `HashAggregate`
- `Exchange`
- `SortMergeJoin`
- `Window`
- `WriteFiles`

The important structural difference from the optimized plan is:

- the naive plan visibly contains repeated `FileScan csv` branches
- it recomputes each branch from the source
- it relies on heavier `SortMergeJoin` style final joining

The naive plan therefore does:
- more repeated I/O
- more repeated preprocessing
- more repeated token generation

## 12. CONCLUSION

The optimized version is faster for structural reasons:

### 1. Less repeated source work

Naive:
- three full input stages of `7.9 GiB`

Optimized:
- two dominant full input stages visible in the stage metrics
- more reuse of intermediate work downstream

### 2. Better reuse of intermediate results

The optimized DAG shows:
- `ReusedExchange`
- `AQEShuffleRead`
- `BroadcastHashJoin`

These are all signs that Spark is avoiding redundant work and improving execution.

### 3. Cheaper final joins

The optimized plan uses `BroadcastHashJoin` for small aggregated datasets.

The naive plan relies on heavier join behavior after repeated rescans.

### 4. Better runtime adaptation

The optimized timeline shows:
- dynamic executor changes
- adaptive behavior

### 5. Shorter long jobs

- naive core jobs: `8.5 min`, `16 min`, `20 min`, `12 min`
- optimized core jobs: `8.5 min`, `13 min`, `4.4 min`, `6.1 min`

So the optimized version substantially reduces the expensive middle part of the execution.
