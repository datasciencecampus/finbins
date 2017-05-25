package uk.gov.ons.dsc.fin

/**
  * Created by noyva on 03/05/2017.
  */

import org.apache.spark.sql.SaveMode
import uk.gov.ons.dsc.utils.stringmetric.StringMetric
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}



object FinBins {


  def main (args:Array[String]):Unit = {

    val appName = "FinBins"
    //val master = args(0)
    val master = "yarn-client"


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fssSchema = StructType(Array(
      StructField("InquiryIDBRCode",StringType,true),StructField("IDBRPeriod",StringType,true),
      StructField("RUReference",IntegerType,false),StructField("FormStatus",StringType,true),
      StructField("Update",StringType,true),StructField("Emplyees",IntegerType,true),
      StructField("DataSource",StringType,true),StructField("FormType",StringType,true),
      StructField("ReceiptDate",StringType,true),StructField("Region",StringType,true),
      StructField("CellSelection",StringType,true),StructField("CurrentSIC",StringType,true),
      StructField("Turnover",StringType,true),StructField("Instance",StringType,true),
      StructField("KeyContributor",StringType,true),
      StructField("q0001",StringType,true),StructField("q0002",StringType,true),StructField("q003",StringType,true),StructField("q0100",StringType,true),
      StructField("q0101",StringType,true),StructField("q0102",StringType,true),StructField("q0103",StringType,true),
      StructField("q0104",StringType,true),StructField("q0105",StringType,true),StructField("q0106",StringType,true),
      StructField("q0107",StringType,true),StructField("q0108",StringType,true),StructField("q0109",StringType,true),
      StructField("q0110",StringType,true),StructField("q0111",StringType,true),StructField("q0112",StringType,true),StructField("q0113",StringType,true),
      StructField("q0114",StringType,true),StructField("q0115",StringType,true),StructField("q0116",StringType,true),
      StructField("q0117",StringType,true),StructField("q0118",StringType,true),StructField("q0119",StringType,true),
      StructField("q0120",StringType,true),StructField("q0121",StringType,true),StructField("q0122",StringType,true),
      StructField("q0123",StringType,true),StructField("q0124",StringType,true),StructField("q0125",StringType,true),
      StructField("q0127",StringType,true),StructField("q0129",StringType,true),StructField("q0130",StringType,true),
      StructField("q0134",StringType,true),StructField("q0135",StringType,true),StructField("q0136",StringType,true),
      StructField("q0137",StringType,true),StructField("q0138",StringType,true),StructField("q0139",StringType,true),
      StructField("q0140",StringType,true),StructField("q0141",StringType,true),StructField("q0170",StringType,true),
      StructField("q0171",StringType,true),StructField("q0172",StringType,true),StructField("q1000",StringType,true),
      StructField("q1001",StringType,true),StructField("q1002",StringType,true),StructField("q1003",StringType,true),
      StructField("q1004",StringType,true),StructField("q1005",StringType,true),StructField("q1006",StringType,true),
      StructField("q1007",StringType,true),StructField("q1008",StringType,true),StructField("q1009",StringType,true),
      StructField("q1010",StringType,true),StructField("q1011",StringType,true),StructField("q1012",StringType,true),
      StructField("q1013",StringType,true),StructField("q1014",StringType,true),StructField("q1015",StringType,true),
      StructField("q1016",StringType,true),StructField("q1017",StringType,true),
      StructField("q1018",StringType,true),StructField("q1019",StringType,true),
      StructField("q1020",StringType,true),StructField("q1021",StringType,true),StructField("q1022",StringType,true),
      StructField("q1023",StringType,true),StructField("q1024",StringType,true),StructField("q1025",StringType,true),
      StructField("q1026",StringType,true),StructField("q1027",StringType,true),StructField("q1028",StringType,true),
      StructField("q1029",StringType,true),StructField("q1030",StringType,true),StructField("q1031",StringType,true),
      StructField("q1032",StringType,true),StructField("q1033",StringType,true),StructField("q1034",StringType,true),
      StructField("q1035",StringType,true),StructField("q1036",StringType,true),StructField("q1037",StringType,true),
      StructField("q1038",StringType,true),StructField("q1039",StringType,true),StructField("q1040",StringType,true),
      StructField("q1041",StringType,true),StructField("q1042",StringType,true),StructField("q1043",StringType,true),
      StructField("q1044",StringType,true),StructField("q1045",StringType,true),StructField("q1046",StringType,true),
      StructField("q1047",StringType,true),StructField("q1048",StringType,true),StructField("q1049",StringType,true),
      StructField("q1050",StringType,true),StructField("q1051",StringType,true),StructField("q1052",StringType,true),
      StructField("q1053",StringType,true),StructField("q1054",StringType,true),StructField("q1055",StringType,true),
      StructField("q1056",StringType,true),StructField("q1057",StringType,true),StructField("q1058",StringType,true),
      StructField("q1059",StringType,true),StructField("q1060",StringType,true),StructField("q1061",StringType,true),
      StructField("q1062",StringType,true),StructField("q1063",StringType,true),StructField("q1064",StringType,true),
      StructField("q1065",StringType,true),StructField("q1066",StringType,true),StructField("q1067",StringType,true),
      StructField("q1068",StringType,true),StructField("q1069",StringType,true),StructField("q1070",StringType,true),
      StructField("q1071",StringType,true),StructField("q1072",StringType,true),StructField("q1073",StringType,true),
      StructField("q1074",StringType,true),StructField("q1075",StringType,true),StructField("q1076",StringType,true),
      StructField("q1077",StringType,true),StructField("q1078",StringType,true),StructField("q1079",StringType,true),
      StructField("q1080",StringType,true),StructField("q1081",StringType,true),StructField("q1082",StringType,true),
      StructField("q1083",StringType,true),StructField("q1084",StringType,true),StructField("q1085",StringType,true),
      StructField("q1086",StringType,true),StructField("q1087",StringType,true),StructField("q1106",StringType,true),
      StructField("q1107",StringType,true),StructField("q1108",StringType,true),StructField("q1109",StringType,true),
      StructField("q1110",StringType,true),StructField("q1111",StringType,true),StructField("q1112",StringType,true),
      StructField("q1119",StringType,true),StructField("q1120",StringType,true),StructField("q1121",StringType,true),
      StructField("q1123",StringType,true),StructField("q1124",StringType,true),
      StructField("q1125",StringType,true),StructField("q1126",StringType,true),StructField("q1127",StringType,true),
      StructField("q1128",StringType,true),StructField("q1129",StringType,true),StructField("q2000",StringType,true),
      StructField("q2001",StringType,true),StructField("q2002",StringType,true),StructField("q2003",StringType,true),
      StructField("q2004",StringType,true),StructField("q2005",StringType,true),StructField("q2006",StringType,true),
      StructField("q2007",StringType,true),StructField("q2008",StringType,true),StructField("q2009",StringType,true),
      StructField("q2010",StringType,true),StructField("q2011",StringType,true),StructField("q2012",StringType,true),
      StructField("q2013",StringType,true),StructField("q2014",StringType,true),StructField("q2015",StringType,true),
      StructField("q2016",StringType,true),StructField("q2017",StringType,true),StructField("q2018",StringType,true),
      StructField("q2019",StringType,true),StructField("q2020",StringType,true),StructField("q2021",StringType,true),
      StructField("q2022",StringType,true),StructField("q2023",StringType,true),StructField("q2024",StringType,true),
      StructField("q2025",StringType,true),StructField("q2026",StringType,true),StructField("q2027",StringType,true),
      StructField("q2028",StringType,true),StructField("q2029",StringType,true),StructField("q2030",StringType,true),
      StructField("q2031",StringType,true),StructField("q2032",StringType,true),StructField("q2033",StringType,true),
      StructField("q2034",StringType,true),StructField("q2035",StringType,true),StructField("q2036",StringType,true),
      StructField("q2037",StringType,true),StructField("q2038",StringType,true),StructField("q2039",StringType,true),
      StructField("q2040",StringType,true),StructField("q2041",StringType,true),StructField("q2104",StringType,true),
      StructField("q2105",StringType,true),StructField("q2107",StringType,true),StructField("q2108",StringType,true),
      StructField("q2111",StringType,true),StructField("q2112",StringType,true),StructField("q2113",StringType,true),
      StructField("q2115",StringType,true),StructField("q2116",StringType,true),StructField("q2117",StringType,true),
      StructField("q2118",StringType,true),StructField("q2119",StringType,true),StructField("q2134",StringType,true),
      StructField("q2135",StringType,true),StructField("q2136",StringType,true),StructField("q3000",StringType,true),
      StructField("q3001",StringType,true),StructField("q3002",StringType,true),StructField("q3003",StringType,true),
      StructField("q3004",StringType,true),StructField("q3005",StringType,true),StructField("q3006",StringType,true),
      StructField("q3007",StringType,true),StructField("q3008",StringType,true),StructField("q3009",StringType,true),
      StructField("q3010",StringType,true),StructField("q3011",StringType,true),StructField("q3012",StringType,true),
      StructField("q3013",StringType,true),StructField("q3014",StringType,true),StructField("q3015",StringType,true),
      StructField("q3016",StringType,true),StructField("q3017",StringType,true),StructField("q3018",StringType,true),
      StructField("q3019",StringType,true),StructField("q3020",StringType,true),StructField("q3021",StringType,true),
      StructField("q3022",StringType,true),StructField("q3023",StringType,true),StructField("q3024",StringType,true),
      StructField("q3025",StringType,true),StructField("q3026",StringType,true),StructField("q3027",StringType,true),
      StructField("q3028",StringType,true),StructField("q3029",StringType,true),StructField("q3030",StringType,true),
      StructField("q3031",StringType,true),StructField("q3032",StringType,true),StructField("q3033",StringType,true),
      StructField("q3034",StringType,true),StructField("q3100",StringType,true),StructField("q3101",StringType,true),
      StructField("q3102",StringType,true),StructField("q3103",StringType,true),StructField("q3104",StringType,true),
      StructField("q3105",StringType,true),StructField("q3106",StringType,true),StructField("q3107",StringType,true),
      StructField("q3108",StringType,true),StructField("q3109",StringType,true),StructField("q3110",StringType,true),
      StructField("q3111",StringType,true),StructField("q3112",StringType,true),
      StructField("q3113",StringType,true),StructField("q3114",StringType,true),
      StructField("q3115",StringType,true),StructField("q3116",StringType,true),StructField("q3117",StringType,true),
      StructField("q3118",StringType,true),StructField("q9100",StringType,true),StructField("q9101",StringType,true),
      StructField("q9102",StringType,true),StructField("q9103",StringType,true),StructField("q9104",StringType,true),
      StructField("q9191",StringType,true),StructField("q9192",StringType,true),StructField("q9200",StringType,true),
      StructField("q9201",StringType,true),StructField("q9202",StringType,true),StructField("q9203",StringType,true),
      StructField("q9204",StringType,true),StructField("q9291",StringType,true),StructField("q9292",StringType,true)))







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



  val fss = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("MODE","DROPMALFORMED").option("quote","'").schema(fssSchema).load("fss.txt")

    fss.write.mode(SaveMode.Overwrite).save("fss0")

    println("fss imported and saved. No of Records:" + fss.count())


  val firms = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").schema(firmsSchema).load("firms.txt")

    firms.write.mode(SaveMode.Overwrite).save("firms0")


    def concCols(col1:String, col2:String):String = {
      if (col1 == null || col2 == null) {
        ""
      }

      else {
        col1.trim() + " " + col2.trim
      }
    }

  val getConcatenated = udf( (first: String, second: String) => concCols(first,second) )

  val firms1 = firms.withColumn("PostCode", getConcatenated(firms.col("name12"), firms.col("name13") ))
  println("No of FCA firms records:"+firms1.count())
    firms1.write.save("firms1")

  val perms= sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").schema(permSchema).load("perm.txt")

  //val firmPerm = perms.join(firms1,"firmId").sort("firmId")
    perms.write.mode(SaveMode.Overwrite).save("perms")


  val idbr= sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("delimiter",":").option("inferSchema","true").load("IDBR_266.txt")
  println("No of IDBR records:"+idbr.count())

    idbr.write.mode(SaveMode.Overwrite).save("idbr0")
  //val fss=  sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("fss.txt")
  //  fss.write.save("fss0")

  //val fssIDBR = fss.join(idbr,fss("RUReference")===idbr("C0"))

  println("No of fss records:"+fss.count())





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





    sqlContext.udf.register("matchPC",matchPC _)
   // sqlContext.udf.register("matchAddr",matchPC _)
   // sqlContext.udf.register("matchName",matchPC _)


    idbr.registerTempTable("IDBR")
    firms1.registerTempTable("FIRMS")

    //println("Test of matchName:"+ matchName("abv","abcd"))

   // val firms_idbr1 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 , FIRMS.name2 , IDBR.C27 , FIRMS.name3 ,  FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 )")

    val firms_idbr1 = sqlContext.sql("SELECT * FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 )")

   // val firms_idbr1 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 FROM IDBR JOIN FIRMS ON IDBR.C37 = FIRMS.PostCode")

    println("No of rec with matching postcode records:"+firms_idbr1.count())

   /* val firms_idbr2 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 ) AND matchAddr (FIRMS.name6, IDBR.C32) ")

    println("No of rec with matching postcode and address records:"+firms_idbr2.count())

    val firms_idbr3 = sqlContext.sql("SELECT IDBR.C37, FIRMS.name12, FIRMS.name13 FROM IDBR, FIRMS WHERE matchPC(FIRMS.name12, FIRMS.name13, IDBR.C37 ) AND matchAddr (FIRMS.name12, FIRMS.name13) AND  matchName (FIRMS.name12, FIRMS.name13)")

    println("No of rec with matching postcode, address and name records:"+firms_idbr3.count())

*/

    firms_idbr1.write.mode(SaveMode.Overwrite).save("firms_idbr1")

    val evalAddr = udf( (addr1:String, addr2:String) => {1.0})
    val evalName = udf ( (name1:String, name2:String) => {matchName(name1,name2)}  )


    val firms_idbr2 = firms_idbr1.withColumn("AddrMatch",evalAddr(firms_idbr1.col("name3"),firms_idbr1.col("name3")))

    println("No of rec with matching address records:"+firms_idbr2.count())

    val firms_idbr3 = firms_idbr2.withColumn("NameMatchScore", evalName(firms_idbr1.col("C27"),firms_idbr1.col("name2")))

    println("No of rec with matching name records:"+firms_idbr3.count())



  }




}
