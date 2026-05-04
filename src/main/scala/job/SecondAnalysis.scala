package job

object SecondAnalysis {
  def main(args: Array[String]): Unit = {
    RedditAnalysisRunner.run(RedditAnalysisRunner.Optimized, args)
  }
}
