package job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
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

  case class Comment(
                      year: Int,
                      subreddit: String,
                      score: Double,
                      tokens: Array[String]
                    )

  case class ResultRow(
                        year: Int,
                        subreddit: String,
                        comments: Long,
                        avg_score: Double,
                        word: String,
                        occurrences: Long,
                        total_tokens: Long,
                        relative_frequency: Double,
                        rank: Int
                      )

  private val stopwords = Set(
    "the","a","an","and","or","but","if","in","on","with","as","at","by","for","to","from","of",
    "is","are","was","were","be","been","have","has","had",
    "i","you","he","she","it","we","they",
    "this","that","these","those",
    "what","when","where","why","how",
    "can","will","just","like","does","did",
    "about","your","their","because",
    "more","most","some","other",
    "really","still","also","well",
    "deleted","removed","http","https"
  )

  def run(jobVersion: JobVersion, args: Array[String]): Unit = {

    val runConfig =
      Commons.parseJobArgs(args, jobVersion.defaultOutputFolder)

    val spark =
      Commons.createSparkSession(
        s"reddit-analysis-${jobVersion.label}",
        runConfig.deploymentMode
      )

    Commons.initializeSparkContext(runConfig.deploymentMode, spark)

    val inputPath =
      Commons.resolvePath(runConfig.deploymentMode, runConfig.inputPath)

    val outputPath =
      Commons.resolvePath(runConfig.deploymentMode, runConfig.outputPath)

    val resultRDD =
      if (jobVersion == Naive)
        buildNaivePlan(spark, inputPath, runConfig.topN)
      else
        buildOptimizedPlan(spark, inputPath, runConfig.topN)

    import spark.implicits._

    resultRDD
      .toDS()
      .orderBy("year", "subreddit", "rank", "word")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    spark.stop()
  }

  private def tokenize(text: String): Array[String] = {
    text
      .toLowerCase
      .replaceAll("http\\S+", " ")
      .replaceAll("[^a-z\\s]", " ")
      .split("\\s+")
      .map(_.trim)
      .filter(token =>
        token.length > 4 &&
          !stopwords.contains(token)
      )
  }

  private def loadComments(
                            spark: SparkSession,
                            inputPath: String
                          ): RDD[Comment] = {

    val df = spark.read
      .option("header", "true")
      .csv(inputPath)

    df.rdd.flatMap { row =>

      try {

        val subreddit =
          Option(row.getAs[String]("subreddit"))
            .map(_.trim)
            .getOrElse("")

        val body =
          Option(row.getAs[String]("body"))
            .map(_.trim)
            .getOrElse("")

        val score =
          Option(row.getAs[String]("score"))
            .map(_.toDouble)

        val year =
          Option(row.getAs[String]("year"))
            .map(_.toInt)

        if (
          subreddit.nonEmpty &&
            body.nonEmpty &&
            body != "[deleted]" &&
            body != "[removed]" &&
            score.isDefined &&
            year.isDefined
        ) {

          val tokens = tokenize(body)

          if (tokens.nonEmpty) {
            Some(
              Comment(
                year.get,
                subreddit,
                score.get,
                tokens
              )
            )
          } else None

        } else None

      } catch {
        case _: Throwable => None
      }
    }
  }

  private def buildNaivePlan(
                              spark: SparkSession,
                              inputPath: String,
                              topN: Int
                            ): RDD[ResultRow] = {

    val commentsA = loadComments(spark, inputPath)
    val commentsB = loadComments(spark, inputPath)
    val commentsC = loadComments(spark, inputPath)

    computeResult(
      commentsA,
      commentsB,
      commentsC,
      topN
    )
  }

  private def buildOptimizedPlan(
                                  spark: SparkSession,
                                  inputPath: String,
                                  topN: Int
                                ): RDD[ResultRow] = {

    val comments =
      loadComments(spark, inputPath)
        .repartition(200)
        .persist(StorageLevel.MEMORY_AND_DISK)

    val result =
      computeResult(
        comments,
        comments,
        comments,
        topN
      )

    comments.unpersist()

    result
  }

  private def computeResult(
                             commentsWord: RDD[Comment],
                             commentsToken: RDD[Comment],
                             commentsStats: RDD[Comment],
                             topN: Int
                           ): RDD[ResultRow] = {

    val wordCounts =
      commentsWord
        .flatMap { c =>
          c.tokens.map(word =>
            ((c.year, c.subreddit, word), 1L)
          )
        }
        .reduceByKey(_ + _)

    val tokenTotals =
      commentsToken
        .map(c =>
          ((c.year, c.subreddit), c.tokens.length.toLong)
        )
        .reduceByKey(_ + _)

    val commentStats =
      commentsStats
        .map(c =>
          ((c.year, c.subreddit), (1L, c.score))
        )
        .reduceByKey {
          case ((countA, scoreA), (countB, scoreB)) =>
            (countA + countB, scoreA + scoreB)
        }
        .mapValues {
          case (count, totalScore) =>
            (count, totalScore / count)
        }

    val joined =
      wordCounts
        .map {
          case ((year, subreddit, word), occurrences) =>
            ((year, subreddit), (word, occurrences))
        }
        .join(tokenTotals)
        .join(commentStats)

    joined.flatMap {
        case ((year, subreddit), (((word, occurrences), totalTokens), (comments, avgScore))) =>

          Some(
            (
              (year, subreddit),
              ResultRow(
                year,
                subreddit,
                comments,
                avgScore,
                word,
                occurrences,
                totalTokens,
                occurrences.toDouble / totalTokens.toDouble,
                0
              )
            )
          )
      }
      .groupByKey()
      .flatMap {
        case (_, rows) =>

          rows.toSeq
            .sortBy(r => (-r.occurrences, r.word))
            .take(topN)
            .zipWithIndex
            .map {
              case (r, idx) =>
                r.copy(rank = idx + 1)
            }
      }
  }
}