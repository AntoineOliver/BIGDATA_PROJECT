package job

object FirstAnalysis {
  def main(args: Array[String]): Unit = {
    RedditAnalysisRunner.run(RedditAnalysisRunner.Naive, args)
  }
}
