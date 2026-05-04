# Reddit Comments Analysis with Apache Spark

## Project overview

This project analyzes a large textual Reddit dataset with Apache Spark and compares two Scala implementations of the same analytical job:
- a non-optimized version
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

## Main analytical job

The main job reads Reddit comments and produces the top words per `(year, subreddit)`.

Main processing steps:
1. read the CSV dataset
2. filter invalid comments
3. normalize the text
4. split the text into tokens
5. aggregate occurrences by `(year, subreddit, word)`
6. aggregate total tokens by `(year, subreddit)`
7. aggregate comment count and average score by `(year, subreddit)`
8. join the aggregated results
9. rank words with a window function
10. keep the top `N` words for each `(year, subreddit)`

### Shuffles

The job includes multiple shuffle-inducing operations:
- aggregation by `(year, subreddit, word)`
- aggregation by `(year, subreddit)` for token totals
- aggregation by `(year, subreddit)` for comment statistics
- joins between the aggregated datasets
- ordering/windowing for top `N`

## Implemented versions

### Non-optimized version
- entrypoint: `job.FirstAnalysis`
- behavior: rescans the source dataset for each analytical branch

### Optimized version
- entrypoint: `job.SecondAnalysis`
- behavior: reuses normalized intermediate data, reduces redundant work, and is better suited for the remote full-data execution

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

A comparison visualisation between the two analysis is in :

-`analyse_history/spark_comparison_visualisation`
