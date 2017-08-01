package uk.gov.ons.dsc.fin

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by noyva on 26/05/2017.
  */
object LinkFSS_IDBR {

  def main (args:Array[String]):Unit = {

    val appName = "FinBins_FSS_IDBR Linking"
    //val master = args(0)
    val master = "yarn"


    val spark = SparkSession
      .builder()
      .master("yarn-client")
      .appName("FinBins-Ingestion")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val idbr = spark.read.load("idbr0")

    val fss = spark.read.load("fss0")

    val joinExpr = fss.col("RUReference") === idbr.col("Id")

    val fss_idbr = fss.join(idbr,joinExpr)

    println("No of fss records:"+fss.count())



    fss_idbr.write.mode(SaveMode.Overwrite).save("fss_idbr")

    println("fss_idbr saved")
  }

}
