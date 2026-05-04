package utils

import org.apache.spark.sql.SparkSession

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths

object Commons {

  final case class JobRunConfig(
      deploymentMode: String,
      inputPath: String,
      outputPath: String,
      topN: Int
  )

  private val supportedModes = Set("local", "remote", "sharedRemote")

  def createSparkSession(appName: String, deploymentMode: String): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(
        "spark.sql.shuffle.partitions",
        if (deploymentMode == "local") "8" else "200"
      )

    if (deploymentMode == "local") {
      val localHistoryDir = Paths.get(Config.localHistoryDirectoryPath)
      Files.createDirectories(localHistoryDir)

      builder.master("local[*]")
      builder.config("spark.driver.host", "127.0.0.1")
      builder.config("spark.driver.bindAddress", "127.0.0.1")
      builder.config("spark.local.hostName", "localhost")
      builder.config("spark.eventLog.enabled", "true")
      builder.config("spark.eventLog.dir", localHistoryDir.toAbsolutePath.toUri.toString)
    }

    builder.getOrCreate()
  }

  def initializeSparkContext(deploymentMode: String, spark: SparkSession): Unit = {
    if (deploymentMode == "remote" || deploymentMode == "sharedRemote") {
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      hadoopConf.set("fs.s3a.endpoint", Config.s3Endpoint)
      hadoopConf.set("fs.s3a.path.style.access", "true")
      hadoopConf.set("fs.s3a.fast.upload", "true")
      hadoopConf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

      resolveRemoteCredentials().foreach { lines =>
        hadoopConf.set("fs.s3a.access.key", lines(0))
        hadoopConf.set("fs.s3a.secret.key", lines(1))

        if (lines.length >= 3 && lines(2).nonEmpty) {
          hadoopConf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
          )
          hadoopConf.set("fs.s3a.session.token", lines(2))
        } else {
          hadoopConf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
          )
        }
      }

      if (resolveRemoteCredentials().isEmpty) {
        hadoopConf.unset("fs.s3a.access.key")
        hadoopConf.unset("fs.s3a.secret.key")
        hadoopConf.unset("fs.s3a.session.token")
        hadoopConf.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
        )
      }
    }
  }

  def resolvePath(deploymentMode: String, path: String): String = {
    if (path.contains("://")) {
      path
    } else {
      deploymentMode match {
        case "local" =>
          toLocalFileUri(path)
        case "sharedRemote" =>
          s"s3a://${Config.s3sharedBucketName.stripPrefix("s3a://").stripSuffix("/")}/${path.stripPrefix("/")}"
        case "remote" =>
          s"s3a://${Config.s3bucketName.stripPrefix("s3a://").stripSuffix("/")}/${path.stripPrefix("/")}"
        case other =>
          throw new IllegalArgumentException(s"Unsupported deployment mode: $other")
      }
    }
  }

  private def toLocalFileUri(path: String): String = {
    val normalized = path.replace("\\", "/")
    val filePath =
      if (normalized.matches("^[A-Za-z]:/.*") || normalized.startsWith("//")) {
        Paths.get(path)
      } else {
        Paths.get(Config.projectDir).resolve(path).normalize()
      }

    filePath.toAbsolutePath.toUri.toString
  }

  def parseJobArgs(args: Array[String], defaultOutputPath: String): JobRunConfig = {
    if (args.isEmpty) {
      throw new IllegalArgumentException(
        "Usage: <local|remote|sharedRemote> [inputPath] [outputPath] [topN]"
      )
    }

    val deploymentMode = args(0)

    if (!supportedModes.contains(deploymentMode)) {
      throw new IllegalArgumentException(
        s"Invalid deployment mode '$deploymentMode'. Supported values: ${supportedModes.mkString(", ")}"
      )
    }

    val defaultInputPath =
      if (deploymentMode == "local") Config.defaultLocalInputPath
      else Config.defaultRemoteInputPath

    val inputPath = args.lift(1).getOrElse(defaultInputPath)
    val outputPath = args.lift(2).getOrElse(defaultOutputPath)
    val topN = args.lift(3).map(_.toInt).getOrElse(Config.defaultTopN)

    JobRunConfig(deploymentMode, inputPath, outputPath, topN)
  }

  private def resolveRemoteCredentials(): Option[Array[String]] = {
    credentialsFromEnvironment()
      .orElse(credentialsFromResourceIfExplicitlyEnabled())
  }

  private def credentialsFromEnvironment(): Option[Array[String]] = {
    val maybeAccessKey = sys.env.get("AWS_ACCESS_KEY_ID").map(_.trim).filter(_.nonEmpty)
    val maybeSecretKey = sys.env.get("AWS_SECRET_ACCESS_KEY").map(_.trim).filter(_.nonEmpty)
    val maybeSessionToken = sys.env.get("AWS_SESSION_TOKEN").map(_.trim).filter(_.nonEmpty)

    for {
      accessKey <- maybeAccessKey
      secretKey <- maybeSecretKey
    } yield Array(accessKey, secretKey, maybeSessionToken.getOrElse(""))
  }

  private def credentialsFromResourceIfExplicitlyEnabled(): Option[Array[String]] = {
    val enabled = sys.env
      .get("USE_AWS_RESOURCE_CREDENTIALS")
      .exists(_.trim.equalsIgnoreCase("true"))

    if (!enabled) {
      None
    } else {
    Option(getClass.getResourceAsStream(Config.credentialsPath))
      .flatMap(readCredentialLines)
    }
  }

  private def readCredentialLines(stream: InputStream): Option[Array[String]] = {
    val source = scala.io.Source.fromInputStream(stream)
    val lines =
      try {
        source.getLines().map(_.trim).filter(_.nonEmpty).toArray
      } finally {
        source.close()
      }

    if (lines.length >= 2) Some(lines) else None
  }
}
