package file.converter.spark.scala.schema

import org.apache.spark.sql.types.StructType

abstract class DataTypes {

  case class stringFileSchema(stringSchema: String) extends DataTypes

  case class structFileSchema(structSchema: StructType) extends DataTypes
}
