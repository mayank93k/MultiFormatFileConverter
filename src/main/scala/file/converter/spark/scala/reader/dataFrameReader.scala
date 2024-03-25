package file.converter.spark.scala.reader

import file.converter.spark.scala.schema.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object dataFrameReader extends DataTypes {

  /**
   * This method takes all the input provided by the user and creates the dataframe after reading the source file.
   *
   * @param sparkSession Creates the sparkSession
   * @param path         Reads the source path
   * @param srcFileType  Source file type
   * @param fileSchema   Provide the schema to load the data or default it will infer the schema from the input file.
   * @param option       readOptions is optional (User should not send any readOption param
   *                     if it is not required) default is Map.empty
   * @return Returns the source dataframe
   */
  def dataFrameReader(sparkSession: SparkSession, path: String, srcFileType: String, fileSchema: DataTypes = null,
                      option: Map[String, String] = Map.empty): DataFrame = {
    fileSchema match {
      case stringFileSchema(stringSchema) => sparkSession.read.format(srcFileType)
        .schema(stringSchema).options(option).load(path)
      case structFileSchema(structSchema) => sparkSession.read.format(srcFileType)
        .schema(structSchema).options(option).load(path)
      case null => sparkSession.read.format(srcFileType).options(option).load(path)
    }
  }
}
