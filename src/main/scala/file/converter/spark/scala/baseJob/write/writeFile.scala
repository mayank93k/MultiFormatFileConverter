package file.converter.spark.scala.baseJob.write

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import file.converter.spark.scala.writer.dataFrameWriter._
import org.apache.spark.sql.DataFrame

trait writeFile extends LazyLogging {

  def writeFileTypeOption(config: Config, destFileType: String): Map[String, String]

  /**
   * This method reads the destination file type and writs the input dataframe in specific file format.
   *
   * @param config       Reads the config
   * @param dataFrame    Input dataframe
   * @param destFileType Destination file format
   */
  def writeDestFile(config: Config, dataFrame: DataFrame, destFileType: String): Unit = {
    val writePath = config.getString("writePath")
    val saveMode = config.getString("saveMode")
    val noOfPartition = config.getInt("noOfPartition")

    val writeOptions = writeFileTypeOption(config, destFileType.toLowerCase)

    /**
     * dataFrameWriter to be updated by user depends type of requirement.
     * noOfPartitions -> noOfPartitions should be provided in config, default is One.
     * saveMode -> saveMode should be provided in config, default is "append"
     * (overwrite: overwrite the existing data.
     * append: append the data.
     * ignore: ignore the operation (i.e. no-op).
     * error or errorifexists: default option, throw an exception at runtime.)
     *
     * writeOptions -> writeOptions is optional (User should not send any writeOptions param if
     * it is not required) default is Map.empty
     * writePath -> Update writePath in jobConfiguration.conf
     */
    dataFrameWriter(dataframe = dataFrame, noOfPartitions = noOfPartition, destFileType = destFileType.toLowerCase,
      saveMode = saveMode, path = writePath, option = writeOptions)
  }
}
