package uk.gov.ons.dsc.fin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by noyva on 17/05/2017.
  */
object Clean {

  def main (args:Array[String]):Unit = {




    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("FinBins_cleaning")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val firms_idbr = spark.read.load("firms_idbr")

    val clean_firms_idbr = firms_idbr.sort(col("nameDist")+col("addrDist"))
      .dropDuplicates(Array("name2"))
      .sort(col("nameDist") + col("addrDist"))

    val filtered_firms_idbr = clean_firms_idbr.filter(col("nameDist") <4 || (col("nameDist") < 6 && col("addrDist") < 2))

    //save cleaned file

    clean_firms_idbr.write.save("firms_idbr_final")

  }

}
