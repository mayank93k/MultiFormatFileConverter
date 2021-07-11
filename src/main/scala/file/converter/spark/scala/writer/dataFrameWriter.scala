package file.converter.spark.scala.writer

import org.apache.spark.sql.DataFrame

object dataFrameWriter {
  /**
   * This method writes the src dataframe to destination path in specified file Format
   *
   * @param dataframe      Source Dataframe
   * @param noOfPartitions No of partition to be created
   * @param destFileType   Destination file format
   * @param saveMode       saveMode should be provided in config, default is "append"
   * @param path           Destination file path
   * @param option         writeOptions is optional (User should not send any writeOptions param if it is not required) default is Map.empty
   */
  def dataFrameWriter(dataframe: DataFrame, noOfPartitions: Int = 1, destFileType: String, saveMode: String = "append", path: String, option: Map[String, String] = Map.empty): Unit = {

    dataframe.coalesce(noOfPartitions).write.format(destFileType).mode(saveMode).options(option).save(path)

  }
}
