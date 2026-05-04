package utils

object Config {
  private def detectProjectDir(): String = {
    sys.env
      .get("PROJECT_ROOT")
      .orElse {
        val location = Option(getClass.getProtectionDomain)
          .flatMap(domain => Option(domain.getCodeSource))
          .map(_.getLocation.toURI)
          .map(uri => new java.io.File(uri))

        location.map { file =>
          val base = if (file.isFile) file.getParentFile else file
          Option(base)
            .flatMap(parent => Option(parent.getParentFile))
            .flatMap(parent => Option(parent.getParentFile))
            .getOrElse(new java.io.File("."))
            .getCanonicalPath
        }
      }
      .getOrElse(new java.io.File(".").getCanonicalPath)
  }

  val projectDir: String = detectProjectDir()
  val s3sharedBucketName: String = "unibo-bd2526-egallinucci-shared"
  val s3bucketName: String = sys.env.getOrElse("PROJECT_S3_BUCKET", "bucket-unique-ao")
  val s3Endpoint: String = "s3.us-east-1.amazonaws.com"
  val credentialsPath: String = "/aws_credentials.txt"
  val localHistoryDirectoryPath: String =
    sys.env.getOrElse("SPARK_LOCAL_HISTORY_DIR", "C:/spark-3.5.3-bin-hadoop3/history")

  val defaultLocalInputPath: String = "datasets/sample.csv"
  val defaultRemoteInputPath: String = "datasets/comments_text.csv"
  val defaultTopN: Int = 10
}
