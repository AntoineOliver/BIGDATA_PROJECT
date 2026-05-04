package job

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import utils.Commons

object RedditAnalysisRunner {

  sealed trait JobVersion {
    def label: String
    def defaultOutputFolder: String
  }

  case object Naive extends JobVersion {
    override val label: String = "naive"
    override val defaultOutputFolder: String = "output/reddit-analysis-naive"
  }

  case object Optimized extends JobVersion {
    override val label: String = "optimized"
    override val defaultOutputFolder: String = "output/reddit-analysis-optimized"
  }

  private val redditSchema = StructType(
    Seq(
      StructField("subreddit", StringType, nullable = true),
      StructField("subreddit_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("body", StringType, nullable = true),
      StructField("controversiality", IntegerType, nullable = true),
      StructField("score", DoubleType, nullable = true),
      StructField("ups", DoubleType, nullable = true),
      StructField("year", IntegerType, nullable = true)
    )
  )

  private val pushshiftSchema = StructType(
    Seq(
      StructField("subreddit", StringType, nullable = true),
      StructField("body", StringType, nullable = true),
      StructField("score", DoubleType, nullable = true),
      StructField("created_utc", LongType, nullable = true)
    )
  )

  private val stopwords = Seq(
    "the", "a", "an", "and", "or", "but", "if", "in", "on", "with", "as", "at", "by", "for", "to", "from", "of",
    "is", "are", "was", "were", "be", "been", "have", "has", "had",
    "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them",
    "this", "that", "these", "those", "there", "here",
    "what", "when", "where", "why", "how",
    "can", "will", "just", "like", "does", "did", "into", "than", "then",
    "about", "your", "their", "because", "which", "while", "where", "whose",
    "more", "most", "some", "such", "other", "much", "many", "only", "very",
    "really", "still", "also", "well", "make", "made", "need", "want", "know",
    "think", "people", "comment", "comments", "post", "reddit", "thread",
    "deleted", "removed", "would", "could", "should", "being", "going", "okay",
    "yeah", "haha", "http", "https"
  )

  private val stopwordsArraySql = stopwords.map(word => s"'$word'").mkString(", ")
  private val normalizedBodySql =
    "trim(regexp_replace(lower(regexp_replace(body, 'http\\\\S+', ' ')), '[^a-z\\\\s]', ' '))"
  private val tokenSql =
    s"filter(split($normalizedBodySql, '\\\\s+'), token -> length(token) > 4 AND NOT array_contains(array($stopwordsArraySql), token))"

  def run(jobVersion: JobVersion, args: Array[String]): Unit = {
    val runConfig = Commons.parseJobArgs(args, jobVersion.defaultOutputFolder)
    val spark = Commons.createSparkSession(s"reddit-analysis-${jobVersion.label}", runConfig.deploymentMode)

    Commons.initializeSparkContext(runConfig.deploymentMode, spark)

    val inputPath = Commons.resolvePath(runConfig.deploymentMode, runConfig.inputPath)
    val outputPath = Commons.resolvePath(runConfig.deploymentMode, runConfig.outputPath)

    val result =
      if (jobVersion == Naive) buildNaivePlan(spark, inputPath, runConfig.topN)
      else buildOptimizedPlan(spark, inputPath, runConfig.topN)

    result
      .orderBy(col("year"), col("subreddit"), col("rank"), col("word"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    println(s"Job version   : ${jobVersion.label}")
    println(s"Deployment    : ${runConfig.deploymentMode}")
    println(s"Input path    : $inputPath")
    println(s"Output path   : $outputPath")
    println(s"Top words kept: ${runConfig.topN}")

    spark.stop()
  }

  private def loadComments(spark: SparkSession, inputPath: String): DataFrame = {
    import spark.implicits._

    val lowerPath = inputPath.toLowerCase

    val comments =
      if (lowerPath.endsWith(".csv")) {
        spark.read
          .option("header", "true")
          .option("multiLine", "false")
          .option("escape", "\"")
          .schema(redditSchema)
          .csv(inputPath)
          .select(
            trim($"subreddit").as("subreddit"),
            trim($"body").as("body"),
            $"score".cast(DoubleType).as("score"),
            $"year".cast(IntegerType).as("year")
          )
      } else {
        spark.read
          .schema(pushshiftSchema)
          .json(inputPath)
          .select(
            trim($"subreddit").as("subreddit"),
            trim($"body").as("body"),
            $"score".cast(DoubleType).as("score"),
            year(from_unixtime($"created_utc".cast(LongType))).cast(IntegerType).as("year")
          )
      }

    comments
      .filter($"subreddit".isNotNull && length($"subreddit") > 0)
      .filter($"body".isNotNull && length(trim($"body")) > 0)
      .filter(!$"body".isin("[deleted]", "[removed]"))
      .filter($"year".isNotNull)
      .filter($"score".isNotNull)
  }

  private def normalizedComments(source: DataFrame): DataFrame = {
    source
      .select(
        col("year"),
        col("subreddit"),
        col("score"),
        expr(tokenSql).as("tokens")
      )
      .filter(size(col("tokens")) > 0)
  }

  private def buildNaivePlan(spark: SparkSession, inputPath: String, topN: Int): DataFrame = {
    val wordCounts = normalizedComments(loadComments(spark, inputPath))
      .select(
        col("year"),
        col("subreddit"),
        explode(col("tokens")).as("word")
      )
      .groupBy("year", "subreddit", "word")
      .agg(count(lit(1)).as("occurrences"))

    val tokenTotals = normalizedComments(loadComments(spark, inputPath))
      .select(
        col("year"),
        col("subreddit"),
        size(col("tokens")).as("token_count")
      )
      .groupBy("year", "subreddit")
      .agg(sum(col("token_count")).cast("long").as("total_tokens"))

    val commentStats = normalizedComments(loadComments(spark, inputPath))
      .select(
        col("year"),
        col("subreddit"),
        col("score")
      )
      .groupBy("year", "subreddit")
      .agg(
        count(lit(1)).as("comments"),
        avg(col("score")).as("avg_score")
      )

    finalizeResult(wordCounts, tokenTotals, commentStats, topN)
  }

  private def buildOptimizedPlan(spark: SparkSession, inputPath: String, topN: Int): DataFrame = {
    val normalized = normalizedComments(loadComments(spark, inputPath))
      .repartition(col("year"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val tokenized = normalized
      .select(
        col("year"),
        col("subreddit"),
        col("score"),
        explode(col("tokens")).as("word")
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    val commentStats = normalized
      .groupBy("year", "subreddit")
      .agg(
        count(lit(1)).as("comments"),
        avg(col("score")).as("avg_score")
      )

    val wordCounts = tokenized
      .groupBy("year", "subreddit", "word")
      .agg(count(lit(1)).as("occurrences"))

    val tokenTotals = tokenized
      .groupBy("year", "subreddit")
      .agg(count(lit(1)).cast("long").as("total_tokens"))

    val result = finalizeResult(wordCounts, tokenTotals, commentStats, topN)

    tokenized.unpersist()
    normalized.unpersist()

    result
  }

  private def finalizeResult(
      wordCounts: DataFrame,
      tokenTotals: DataFrame,
      commentStats: DataFrame,
      topN: Int
  ): DataFrame = {
    val rankingWindow = Window
      .partitionBy(col("year"), col("subreddit"))
      .orderBy(col("occurrences").desc, col("word").asc)

    wordCounts
      .join(tokenTotals, Seq("year", "subreddit"), "inner")
      .join(commentStats, Seq("year", "subreddit"), "inner")
      .withColumn("relative_frequency", round(col("occurrences") / col("total_tokens"), 8))
      .withColumn("avg_score", round(col("avg_score"), 6))
      .withColumn("rank", row_number().over(rankingWindow))
      .filter(col("rank") <= lit(topN))
      .select(
        col("year"),
        col("subreddit"),
        col("comments"),
        col("avg_score"),
        col("word"),
        col("occurrences"),
        col("total_tokens"),
        col("relative_frequency"),
        col("rank")
      )
  }
}
