package uk.gov.ons.dsc.fin

import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.dsc.utils.stringmetric.StringMetric
import org.apache.spark.sql.functions._


/**
  * Created by noyva on 17/05/2017.
  */
object Clean {

  def main (args:Array[String]):Unit = {

    val appName = "FinBins_cleaning"
    //val master = args(0)
    val master = "yarn-client"


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val firms_idbr = sqlContext.read.load("firms_idbr")

    val clean_firms_idbr = firms_idbr.sort(col("nameDist")+col("addrDist"))
      .dropDuplicates(Array("name2"))
      .sort(col("nameDist") + col("addrDist"))

    val filtered_firms_idbr = clean_firms_idbr.filter(col("nameDist") <4 || (col("nameDist") < 6 && col("addrDist") < 2))

    //save cleaned file

    clean_firms_idbr.write.save("firms_idbr_final")

  }

}
