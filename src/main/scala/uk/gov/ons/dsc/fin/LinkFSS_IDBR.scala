package uk.gov.ons.dsc.fin

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by noyva on 26/05/2017.
  */
object LinkFSS_IDBR {

  def main (args:Array[String]):Unit = {

    val appName = "FinBins_FSS_IDBR linking"
    //val master = args(0)
    val master = "yarn-client"


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val idbr = sqlContext.read.load("idbr0")

    val fss = sqlContext.read.load("fss0")

    val joinExpr = fss.col("RUReference") === idbr.col("C0")

    val fss_idbr = fss.join(idbr,joinExpr)

    println("No of fss records:"+fss.count())

    fss_idbr.write.mode(SaveMode.Overwrite).save("fss_idbr")

    println("fss_idbr saved")
  }

}
