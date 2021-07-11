package file.converter.spark.scala.baseJob.read

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import file.converter.spark.scala.reader.dataFrameReader._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait readFile extends LazyLogging {

  def readFileTypeOption(config: Config, srcFileType: String): Map[String, String]

  /**
   * This method reads the source file type and creates it as dataframe.
   *
   * @param config       Reads the config
   * @param sparkSession Creates the spark session
   * @param srcFileType  Reads the source file type
   * @return Returns the dataframe after reading the src file
   */
  def readSrcFile(config: Config, sparkSession: SparkSession, srcFileType: String): DataFrame = {
    val readPath = config.getString("readPath")

    val readOptions = readFileTypeOption(config, srcFileType.toLowerCase)

    /**
     * dataFrameReader to be updated by user depends type of requirement.
     * readOptions -> readOptions is optional (User should not send any readOption param if it is not required) default is Map.empty
     * fileSchema -> fileSchema is optional (User should create file schema and pass its param) default is null
     * (schema: ("a INT, b STRING, c DOUBLE")
     * schema: StructType )
     *
     * readPath -> Update readPath in jobConfiguration.conf
     */
    val readDataFile = dataFrameReader(sparkSession = sparkSession, path = readPath, srcFileType = srcFileType.toLowerCase, option = readOptions)
    readDataFile
  }
}
