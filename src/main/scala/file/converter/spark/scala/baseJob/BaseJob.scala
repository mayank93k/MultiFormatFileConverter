package file.converter.spark.scala.baseJob

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import file.converter.spark.scala.baseJob.read.readFile
import file.converter.spark.scala.baseJob.write.writeFile
import file.converter.spark.scala.fileConverter.fileConverter.fileConverter
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

trait BaseJob extends readFile with writeFile with LazyLogging {
  /**
   * Function to convert Seq[(String, String)] to Map[String, String]
   */
  private val convertToMap: mutable.Seq[(String, String)] => Map[String, String] = Map[String, String]

  /**
   * This method reads the config and returns the spark file read option in key, value format
   *
   * @param config   Reads the config
   * @param fileType Reads the type of input file
   * @return Returns the read option in the form of Map(key, value) pair
   */
  def readFileTypeOption(config: Config, fileType: String): Map[String, String] = {
    fileType match {
      case "csv" => readOptionsFromConfig(config, "readOption.csv", convertToMap)
      case "json" => readOptionsFromConfig(config, "readOption.json", convertToMap)
      case "orc" => readOptionsFromConfig(config, "readOption.orc", convertToMap)
      case "parquet" => readOptionsFromConfig(config, "readOption.parquet", convertToMap)
      case "avro" => readOptionsFromConfig(config, "readOption.avro", convertToMap)
      case _ => throw new IllegalArgumentException("File type not specified")
    }
  }

  /**
   * This method reads the config and creates the map(key, value) pair
   *
   * @param config       Reads the config
   * @param readOption   Reads the option on the basis of type of file
   * @param mapConverter Function to convert Seq[(String, String)] to Map[String, String]
   * @return Returns the option as Map[String, String]
   */
  private def readOptionsFromConfig(config: Config, readOption: String,
                                    mapConverter: mutable.Seq[(String, String)] => Map[String, String]): Map[String, String] = {
    val options: mutable.Seq[(String, String)] = config.getStringList(readOption).asScala.map(x => {
      val arrayValue = x.split("=")
      val key: String = arrayValue(0)
      val value: String = arrayValue(1)
      (key, value)
    })

    mapConverter(options)
  }

  /**
   * This method reads the config and returns the spark file write option in key, value format
   *
   * @param config   Reads the config
   * @param fileType Reads the type of input file
   * @return Returns the write option in the form of Map(key, value) pair
   */
  def writeFileTypeOption(config: Config, fileType: String): Map[String, String] = {
    fileType match {
      case "csv" => readOptionsFromConfig(config, "writeOption.csv", convertToMap)
      case "json" => readOptionsFromConfig(config, "writeOption.json", convertToMap)
      case "orc" => readOptionsFromConfig(config, "writeOption.orc", convertToMap)
      case "parquet" => readOptionsFromConfig(config, "writeOption.parquet", convertToMap)
      case "avro" => readOptionsFromConfig(config, "writeOption.avro", convertToMap)
      case _ => throw new IllegalArgumentException("File type not specified")
    }
  }

  /**
   * This method calls the readSrcFile on the basis of srcFileFormat provided by the user,
   * and throws exception if not provided
   *
   * @param config       Reads the config
   * @param sparkSession sparkSession
   * @return
   */
  def fileReader(config: Config, sparkSession: SparkSession): DataFrame = {
    val srcFileFormat = config.getString("srcFileType").toLowerCase

    logger.info("Reading file type as: " + srcFileFormat)

    srcFileFormat match {
      case "csv" | "json" | "orc" | "parquet" | "avro" => readSrcFile(config, sparkSession, srcFileFormat)
      case _ => throw new IllegalArgumentException("Read file type not specified")
    }
  }

  /**
   * This method calls the writeDestFile on the basis of destFileFormat provided by the user,
   * and throws exception if not provided
   *
   * @param config    Reads the config
   * @param dataFrame sparkSession
   */
  def fileWriter(config: Config, dataFrame: DataFrame): Unit = {
    val destFileFormat = config.getString("destFileType").toLowerCase

    logger.info("Writing file type as: " + destFileFormat)

    destFileFormat match {
      case "csv" | "json" | "orc" | "parquet" | "avro" => writeDestFile(config, dataFrame, destFileFormat)
      case _ => throw new IllegalArgumentException("Write file type not specified")
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      sys.exit()
    }
    val config = getConfigs(args)._1
    val sparkSession = getConfigs(args)._2
    val srcFileFormat = config.getString("srcFileType")
    try {
      fileConverter(config, sparkSession)
    }
    catch {
      case exception: Throwable => logger.error(s"Reading of file type: $srcFileFormat " +
        s"is unsuccessful: " + exception.getMessage)
    }
    finally {
      sparkSession.stop()
    }
  }

  /**
   * This method takes the runtime arguments and update the config value.
   * Also its calls the createSparkSession method which initializes spark session.
   *
   * @param getRunTimeParam Accepts the run time arguments
   * @return Returns the tuple of config and sparkSession
   */
  private def getConfigs(getRunTimeParam: Array[String]): (Config, SparkSession) = {
    logger.info("Reading fileConverter.conf file started")
    val conf: Config = ConfigFactory.load("fileConverter.conf")

    val updatedConfig = filterRunTimeParam(conf, getRunTimeParam)
    logger.info("Reading fileConverter.conf file finished")

    val spark = createSparkSession(updatedConfig)
    (updatedConfig, spark)
  }

  /**
   * This method creates sparkSession.
   *
   * @param config Reads the config param to get the values from the config
   * @return Returns the SparkSession
   */
  private def createSparkSession(config: Config): SparkSession = {
    val getSparkName = config.getString("sparkOptions.spark.app.name")
    val getMaster = config.getString("sparkOptions.spark.master")

    logger.info("SparkSession being initialized")
    SparkSession.builder().master(getMaster).appName(getSparkName).getOrCreate()
  }

  /**
   * This method filters the RunTime Param provided by the user and update into config
   *
   * @param config Reads the config
   * @param args   Reads the runtime config
   * @return Returns the Config
   */
  private def filterRunTimeParam(config: Config, args: Array[String]): Config = {
    args.foldLeft(config) {
      (result: Config, current: String) =>
        val param: Array[String] = current.split("=")
        result.withValue(param(0), ConfigValueFactory.fromAnyRef(param(1)))
    }
  }

}
