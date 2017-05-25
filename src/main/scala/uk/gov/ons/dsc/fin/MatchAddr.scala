package uk.gov.ons.dsc.fin

import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.dsc.utils.stringmetric.StringMetric
import org.apache.spark.sql.functions.udf


/**
  * Created by noyva on 15/05/2017.
  */
object MatchAddr {

  def main (args:Array[String]):Unit = {

    val appName = "FinBins_Name_Addr_Matching"
    //val master = args(0)
    val master = "yarn-client"


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val firms_IDBR = sqlContext.read.load("firms_idbr1")

    def matchName1 (Name1:String, Name2:String) :Int = {
      if (Name1 == null || Name2 == null) {
        100
      } else {
        val in1:Array[String] = Name1.replaceAll("[.']","").toUpperCase.split(" ")
        val in2:Array[String] = Name2.replaceAll("[.']","").toUpperCase.split(" ")

        val dis2 = for (l<-in1 ) yield
          {

            val dis1 = for (m<-in2) yield
              {
                if (l == "&" || l=="THE" || ((l=="INT"||l=="INTERNATIONAL") && (m=="INTERNATIONAL"|| m=="INT")) || ((l == "LIMITED" || l=="LTD" ||  l=="LTD." ) && (m == "LIMITED" || m=="LTD" ||  m=="LTD.")) || ((l == "CO" || l=="COMPANY" ) && (m == "CO" || m=="COMPANY" ))) {
                  0
                }
                else {
                  StringMetric.compareWithLevenshtein(l.toCharArray, m.toCharArray).getOrElse(50)
                }

              }

            dis1.min
          }

          dis2.sum + Math.abs(in1.length - in2.length)*3


      }
    }

    def matchAddr1 (addr1:String, addr2:String) :Int = {
      if (addr1 == null || addr2 == null) {
        100
      } else {
        val in1:Array[String] = addr1.replaceAll("[.']","").toUpperCase.split(" ")
        val in2:Array[String] = addr2.replaceAll("[.']","").toUpperCase.split(" ")

        val dis2 = for (l<-in1 ) yield
          {

            val dis1 = for (m<-in2) yield
              {
                if (l=="UNIT") {
                  0
                }
                else {
                  StringMetric.compareWithLevenshtein(l.toCharArray, m.toCharArray).getOrElse(50)
                }

              }

            dis1.min
          }

        dis2.sum + Math.abs(in1.length - in2.length)*3


      }
    }

    val evalName1 = udf ( (name1:String, name2:String) => {matchName1(name1,name2)}  )
    val evalAddr1 = udf ( (a1:String, a2:String) => {matchName1(a1,a2)}  )


    val newFirms_IDBR = firms_IDBR.withColumn("nameDist",evalName1(firms_IDBR.col("name2"),firms_IDBR.col("C26")))
                                  .withColumn("addrDist",evalAddr1(firms_IDBR.col("name6"),firms_IDBR.col("C32")))

    //save intermediate results
    newFirms_IDBR.write.save("firms_idbr")





  }

}
