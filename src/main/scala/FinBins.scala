package uk.gov.ons.dsc.fin


/**
  * Created by noyva on 03/05/2017.
  */

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf

object FinBins {


  def main (args:Array[String]):Unit = {

    val appName = "FinBins"
    //val master = args(0)
    val master = "yarn-client"


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val firmsSchema = StructType(Array(
      StructField("firmId",IntegerType,true),
      StructField("name2",StringType,true),
      StructField("name3",StringType,true),
      StructField("name4",StringType,true),
      StructField("name5",StringType,true),
      StructField("name6",StringType,true),
      StructField("name7",StringType,true),
      StructField("name8",StringType,true),
      StructField("name9",StringType,true),
      StructField("name10",StringType,true),
      StructField("name11",StringType,true),
      StructField("name12",StringType,true),
      StructField("name13",StringType,true),
      StructField("name14",StringType,true),
      StructField("name15",StringType,true),
      StructField("name16",StringType,true),
      StructField("name17",StringType,true),
      StructField("name18",StringType,true),
      StructField("name19",StringType,true),
      StructField("name20",StringType,true),
      StructField("name21",StringType,true),
      StructField("name22",StringType,true),
      StructField("name23",StringType,true),
      StructField("name24",StringType,true)))




    val permSchema = StructType(Array(
      StructField("firmId",IntegerType,true),
      StructField("name2",StringType,true),
      StructField("name3",StringType,true),
      StructField("name4",StringType,true),
      StructField("name5",StringType,true),
      StructField("name6",StringType,true),
      StructField("name7",StringType,true)))



  val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("fss.txt")

  val firms = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").schema(firmsSchema).load("firms.txt")

  val perms= sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").schema(permSchema).load("perm.txt")

  val firmPerm = perms.join(firms,"firmId").sort("firmId")


  val idbr= sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("delimiter",":").option("inferSchema","true").load("IDBR_266.txt")

  val fss=  sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("fss.txt")

  //val fssIDBR = fss.join(idbr,fss("RUReference")===idbr("C0"))

  println("No of fss records:"+fss.count())


  def matchPC(PC1:String, PC2:String, PC3:String):Boolean = {
    if (PC1 == null || PC2 == null || PC3 == null) {
      false
    }
    else if (PC3.trim == PC1.trim() + PC2.trim() ) {
      true
    }
    else {
      false
    }
  }

    sqlContext.udf.register("matchPC",matchPC _)


idbr.registerTempTable("idbr")
firms.registerTempTable("firms")

val firms_idbr = sqlContext.sql("SELECT IDBR.C37, FIRMS.NAME12, FIRMS.NAME13 FROM IDBR, FIRMS WHERE matchPC(FIRMS.NAME12, FIRMS.NAME13, IDBR.C37 )")

    println("No of matching postcode records:"+firms_idbr.count())

    firms_idbr.show()

  }




}
