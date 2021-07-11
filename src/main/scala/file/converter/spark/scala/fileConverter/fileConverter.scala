package file.converter.spark.scala.fileConverter

import com.typesafe.config.Config
import file.converter.spark.scala.baseJob.BaseJob
import org.apache.spark.sql.SparkSession

object fileConverter extends BaseJob {
  /**
   * This method converts the specific source file format to the desired destination file format.
   *
   * @param config Reads the updated config
   * @param spark  Initializes spark session
   */
  def fileConverter(config: Config, spark: SparkSession): Unit = {

    /**
     * Update readFile for any change in way of reading a file and
     * Update writeFile for any change in way of writing a file
     */
    val srcDataFrame = fileReader(config, spark)
    fileWriter(config, srcDataFrame)

  }

}
