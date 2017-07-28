package uk.gov.ons.dsc.fin

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import uk.gov.ons.dsc.utils.stringmetric.StringMetric


/**
  * Created by noyva on 15/05/2017.
  */
object LinkFCA_IDBR {

  def main (args:Array[String]):Unit = {

    val appName = "FinBins_LinkFCA_IDBR"
    //val master = args(0)
    val master = "yarn-client"


    val spark = SparkSession
      .builder()
      .master("yarn-client")
      .appName("FinBins-LinkFCA_IDBR")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()




    def matchPC(PC1:String, PC2:String, PC3:String):Boolean = {
      if (PC1 == null || PC2 == null || PC3 == null) {
        false
      }
      // else if (PC3.replaceAll(" ","").toUpperCase == (PC1 + PC2).replaceAll(" ","").toUpperCase ) {
      else if ( PC3.contains(PC1.trim) && PC3.contains(PC2.trim) ) {
        true
      }
      else {
        false
      }
    }


    def matchAddr(Addr1:String, Addr2:String) :Boolean = {

      if (Addr1.replaceAll(" ","").toUpperCase == Addr2.replaceAll(" ","").toUpperCase) {
        true
      } else {
        false
      }
    }


    def matchName (Name1:String, Name2:String) :Int = {
      if (Name1 == null || Name2 == null) {
        0
      } else {
        val dist =  StringMetric.compareWithLevenshtein(Name1.toCharArray, Name2.toCharArray)
        println (Name1 + " " + Name2 + ":" + dist)
        dist.get
      }
    }





    spark.udf.register("matchPC",matchPC _)
    // sqlContext.udf.register("matchAddr",matchPC _)
    // sqlContext.udf.register("matchName",matchPC _)

    val idbr = spark.read.load("idbr0")
    val firms1 = spark.read.load("firms1")

    idbr.createGlobalTempView("IDBR")
    firms1.createGlobalTempView("FIRMS")

    //println("Test of matchName:"+ matchName("abv","abcd"))

    // val firms_idbr1 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 , FIRMS.name2 , IDBR.C27 , FIRMS.name3 ,  FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 )")

    val firms_idbr1 = spark.sql("SELECT * FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 )")

    println(" Matching postcodes finished ...")

    // val firms_idbr1 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 FROM IDBR JOIN FIRMS ON IDBR.C37 = FIRMS.PostCode")

    //***   println("No of rec with matching postcode records:"+firms_idbr1.count())

    /* val firms_idbr2 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 ) AND matchAddr (FIRMS.name6, IDBR.C32) ")

     println("No of rec with matching postcode and address records:"+firms_idbr2.count())

     val firms_idbr3 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 ) AND matchAddr (FIRMS.name12, FIRMS.name13) AND  matchName (FIRMS.name12, FIRMS.name13)")

     println("No of rec with matching postcode, address and name records:"+firms_idbr3.count())

 */

      firms_idbr1.write.mode(SaveMode.Overwrite).save("firms_idbr1")

    val evalAddr = udf( (addr1:String, addr2:String) => {1.0})
    val evalName = udf ( (name1:String, name2:String) => {matchName(name1,name2)}  )


    //******   val firms_idbr2 = firms_idbr1.withColumn("AddrMatch",evalAddr(firms_idbr1.col("name3"),firms_idbr1.col("name3")))

    //****   println("No of rec with matching address records:"+firms_idbr2.count())

    //*****    val firms_idbr3 = firms_idbr2.withColumn("NameMatchScore", evalName(firms_idbr1.col("C27"),firms_idbr1.col("name2")))

    //****   println("No of rec with matching name records:"+firms_idbr3.count())



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

          // dis2.sum + Math.abs(in1.length - in2.length)*3

         dis2.sum
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


    val newFirms_IDBR = firms_idbr1.withColumn("nameDist",evalName1(firms_idbr1.col("name2"),firms_idbr1.col("C26")))
                                  .withColumn("addrDist",evalAddr1(firms_idbr1.col("name6"),firms_idbr1.col("C32")))

    //save intermediate results
    newFirms_IDBR.write.save("firms_idbr")





  }

}
