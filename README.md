# Reddit Comments Analysis with Apache Spark

## Project overview

This project analyzes a large textual Reddit dataset with Apache Spark and compares two Scala implementations of the same analytical job:
- a naive version
- an optimized version

The goal is to compute, for each `(year, subreddit)`, the top `N` most frequent words together with:
- the number of valid comments
- the average comment score
- the total number of retained tokens
- the relative frequency of each word

The project is designed for:
- local development and debugging on a sample
- remote execution on AWS EMR over the full dataset stored in S3
- performance comparison through Spark History Server

## Dataset

Chosen dataset:
- [Reddit Comments Text - Kaggle](https://www.kaggle.com/datasets/abianchi/reddit-comments-text)

Main file used:
- `comments_text.csv`

Current full dataset used by the project:
- `datasets/comments_text.csv`
- size: about `8.53 GB`

### Main job pipeline

The job performs:

- Load CSV from S3 or local
- Filter invalid comments
- Tokenize text
- Compute:
   - word counts (year, subreddit, word)
   - token totals (year, subreddit)
   - comment stats (year, subreddit)
- Join all aggregations
- Rank words per group
- Write final CSV to S3

#### Spark execution characteristics

The job contains multiple shuffles, due to:

- groupByKey / reduceByKey
- joins between aggregated datasets
- ranking per (year, subreddit)
- repartitioning in optimized version


## Implemented versions

### Naive version
- Entry point: `job.FirstAnalysis`
- Logic:
  - reloads dataset multiple times
  - recomputes independent branches
  - no reuse of intermediate RDDs

- Problems:
  - multiple full scans of dataset
  - duplicated computation
  - higher shuffle volume

### Optimized version
- entrypoint: `job.SecondAnalysis`
- Improvements:
  - single normalized RDD
  - repartition(200)
  - persist(MEMORY_AND_DISK)
  - reuse of intermediate data

- Benefits:
  - fewer full scans
  - reduced shuffle pressure
  - faster execution
  
### Output comparison
- entrypoint: `job.CompareOutputs`

This utility checks that both versions return exactly the same result.

## Repository structure

- `src/main/scala/job/FirstAnalysis.scala`: naive job entrypoint
- `src/main/scala/job/SecondAnalysis.scala`: optimized job entrypoint
- `src/main/scala/job/RedditAnalysisRunner.scala`: shared Spark logic
- `src/main/scala/job/CompareOutputs.scala`: result comparison
- `src/main/scala/utils/Commons.scala`: argument parsing, Spark session setup, path resolution, remote S3 support
- `src/main/scala/utils/Config.scala`: project defaults
- `datasets/`: local datasets
- `output/`: local outputs
- `src/main/resources/aws_credentials.txt`: optional AWS credentials resource if explicitly enabled

### Local result

The output CSV contains:
- `year`
- `subreddit`
- `comments`
- `avg_score`
- `word`
- `occurrences`
- `total_tokens`
- `relative_frequency`
- `rank`

### Explanation

The more details explanations are in :

-`explanation.md`
