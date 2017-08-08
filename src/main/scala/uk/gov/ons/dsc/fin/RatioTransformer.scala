package uk.gov.ons.dsc.fin


import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, DoubleType, StructType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset}


class RatioTransformer ( override val uid: String)  extends Transformer  {

  //parameter for input columns
  final val featureCol1:Param[String] = new Param[String] (this, "featureCol1", "input column 1 name")
  final def getInputCol1: String = $(featureCol1)
  final def setInputCol1 (col:String):RatioTransformer =  set(featureCol1,col)

  final val featureCol2:Param[String] = new Param[String] (this, "featureCol2", "input column 2 name")
  final def getInputCol2: String = $(featureCol2)
  final def setInputCol2 (col:String):RatioTransformer =  set(featureCol2,col)

  //parameter for the output column
  final val outputCol:Param[String] = new Param [String] (this,"outputCol","output column name")
  final def getOutputCol:String = $(outputCol)
  final def setOutputCol (value:String) :RatioTransformer = set (outputCol,value)

  import org.apache.spark.sql.functions.udf
  val calcRatio = udf { (in1:Double, in2:Double) => if (in1 != 0) {in2/in1} else 0.0}


  override def transformSchema(schema:StructType ) :StructType =  {

    // Validate input type.
    // Input type validation is technically optional, but it is a good practice since it catches
    // schema errors early on.

    val actualDataType1 = schema($(featureCol1)).dataType
    require(actualDataType1.equals(DataTypes.DoubleType),
      s"Column ${$(featureCol1)} must be DoubleType but was actually $actualDataType1.")

    val actualDataType2 = schema($(featureCol2)).dataType
    require(actualDataType2.equals(DataTypes.DoubleType),
      s"Column ${$(featureCol2)} must be DoubleType but was actually $actualDataType2.")

    if (schema.fieldNames.contains(outputCol)) {
      throw new IllegalArgumentException(s"Output column $outputCol already exists.")
    }

    StructType(schema.fields :+ new StructField($(outputCol), DoubleType, true))
  }


  override def transform (dataset: Dataset[_]): DataFrame = {


    dataset.select(col("*"), calcRatio(col($(featureCol1)), col($(featureCol2))).as($(outputCol)))

  }

  override def copy(extra:ParamMap): Transformer = defaultCopy(extra)





}
