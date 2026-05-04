package job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import utils.Commons

object CompareOutputs {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new IllegalArgumentException(
        "Usage: CompareOutputs <local|remote|sharedRemote> <outputPathA> <outputPathB>"
      )
    }

    val deploymentMode = args(0)
    val outputPathA = Commons.resolvePath(deploymentMode, args(1))
    val outputPathB = Commons.resolvePath(deploymentMode, args(2))

    val spark = Commons.createSparkSession("reddit-analysis-compare", deploymentMode)
    Commons.initializeSparkContext(deploymentMode, spark)

    val orderedColumns = Seq(
      "year",
      "subreddit",
      "comments",
      "avg_score",
      "word",
      "occurrences",
      "total_tokens",
      "relative_frequency",
      "rank"
    )

    val left = spark.read.option("header", "true").csv(outputPathA).select(orderedColumns.map(col): _*)
    val right = spark.read.option("header", "true").csv(outputPathB).select(orderedColumns.map(col): _*)

    val onlyLeft = left.exceptAll(right).count()
    val onlyRight = right.exceptAll(left).count()

    if (onlyLeft == 0 && onlyRight == 0) {
      println("Outputs are identical.")
    } else {
      println(s"Outputs differ. Rows only in A: $onlyLeft, rows only in B: $onlyRight")
      sys.exit(1)
    }

    spark.stop()
  }
}
