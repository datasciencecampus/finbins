package uk.gov.ons.dsc.fin

/**
  * Created by noyva on 03/05/2017.
  */

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}



object Ingestion {


  def main (args:Array[String]):Unit = {


    val spark = SparkSession
      .builder()
      .master("yarn")
    //  .deploy-mode("client")
      .appName("FinBins-Ingestion")
    //  .set("spark.cores.max", "25")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


   // val sqlContext = spark.sqlContext





  val fss = spark.read
                       .format("com.databricks.spark.csv")
                       .option("header","true")
         //              .option("MODE","DROPMALFORMED")
         //              .option("quote","'")
                        .schema(fssSchema)
         //               .option("inferSchema","true")
                       .option("delimiter","|")
                       .load("fss.csv")

    fss.write.mode(SaveMode.Overwrite).save("fss0")

    println("fss imported and saved. No of Records:" + fss.count())


  val firms = spark.read
                       .format("com.databricks.spark.csv")
                       .option("header","true")
                       .option("delimiter","|")
                       .schema(firmsSchema)
                       .load("firms.csv")

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
    firms1.write.mode(SaveMode.Overwrite).save("firms1")

  val perms= spark.read
                  .format("com.databricks.spark.csv")
                  .option("header","true")
                  .option("delimiter","|")
                  .schema(permSchema)
                  .load("perm.csv")

  //val firmPerm = perms.join(firms1,"firmId").sort("firmId")
    perms.write.mode(SaveMode.Overwrite).save("perms")


  val idbr= spark.read
                 .format("com.databricks.spark.csv")
                 .option("header","false")
                 .option("delimiter",":")
                //         .option("inferSchema","true")
                 .schema(idbrSchema)
                 .load("IDBR_266.csv")

  println("No of IDBR records:"+idbr.count())

    idbr.write.mode(SaveMode.Overwrite).save("idbr0")
  //val fss=  sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("fss.txt")
  //  fss.write.save("fss0")

  //val fssIDBR = fss.join(idbr,fss("RUReference")===idbr("C0"))

  println("No of fss records:"+fss.count())




  }

  val idbrSchema = StructType(Array(
    StructField("Id",LongType,false),
    StructField("C1",StringType,true),StructField("C2",IntegerType,true),StructField("C3",IntegerType,true),StructField("C4",IntegerType,true),StructField("C5",IntegerType,true),
    StructField("C6",DoubleType,true),StructField("C7",DoubleType,true),StructField("C8",DoubleType,true),StructField("C9",DoubleType,true),StructField("C10",DoubleType,true),
    StructField("C11",DoubleType,true),StructField("C12",DoubleType,true),StructField("C13",DoubleType,true),StructField("C14",LongType,true),StructField("C15",DoubleType,true),
    StructField("C16",StringType,true),StructField("C17",StringType,true),StructField("C18",StringType,true),StructField("C19",DoubleType,true),StructField("C20",DoubleType,true),
    StructField("C21",DoubleType,true),StructField("C22",IntegerType,true),StructField("C23",StringType,true),StructField("C24",StringType,true),StructField("C25",StringType,true),
    StructField("C26",StringType,true),StructField("C27",StringType,true),StructField("C28",StringType,true),StructField("C29",StringType,true),StructField("C30",StringType,true),
    StructField("C31",StringType,true),StructField("C32",StringType,true),StructField("C33",StringType,true),StructField("C34",StringType,true),StructField("C35",StringType,true),
    StructField("C36",StringType,true),StructField("C37",StringType,true),StructField("C38",StringType,true),StructField("C39",StringType,true),StructField("C40",StringType,true),
    StructField("C41",StringType,true),StructField("C42",StringType,true),StructField("C43",StringType,true),StructField("C44",StringType,true),StructField("C45",StringType,true),
    StructField("C46",DoubleType,true),StructField("C47",IntegerType,true),StructField("C48",StringType,true),StructField("C49",StringType,true)
  ))

  val fssSchema = StructType(Array(
    StructField("InquiryIDBRCode",StringType,true),StructField("IDBRPeriod",IntegerType,true),
    StructField("RUReference",LongType,false),StructField("FormStatus",IntegerType,true),
    StructField("Update",StringType,true),StructField("Emplyees",IntegerType,true),
    StructField("DataSource",IntegerType,true),StructField("FormType",IntegerType,true),
    StructField("ReceiptDate",StringType,true),StructField("Region",StringType,true),
    StructField("CellSelection",IntegerType,true),StructField("CurrentSIC",StringType,true),
    StructField("Turnover",IntegerType,true),StructField("Instance",IntegerType,true),
    StructField("KeyContributor",StringType,true),
    StructField("q0001",IntegerType,true),StructField("q0002",IntegerType,true),StructField("q0003",IntegerType,true),StructField("q0100",IntegerType,true),
    StructField("q0101",StringType,true),StructField("q0102",StringType,true),StructField("q0103",IntegerType,true),
    StructField("q0104",StringType,true),StructField("q0105",IntegerType,true),StructField("q0106",IntegerType,true),
    StructField("q0107",StringType,true),StructField("q0108",IntegerType,true),StructField("q0109",IntegerType,true),
    StructField("q0110",StringType,true),StructField("q0111",IntegerType,true),StructField("q0112",IntegerType,true),StructField("q0113",IntegerType,true),
    StructField("q0114",IntegerType,true),StructField("q0115",IntegerType,true),StructField("q0116",StringType,true),
    StructField("q0117",IntegerType,true),StructField("q0118",StringType,true),StructField("q0119",IntegerType,true),
    StructField("q0120",IntegerType,true),StructField("q0121",IntegerType,true),StructField("q0122",IntegerType,true),
    StructField("q0123",IntegerType,true),StructField("q0124",StringType,true),StructField("q0125",IntegerType,true),
    StructField("q0127",StringType,true),StructField("q0129",IntegerType,true),StructField("q0130",IntegerType,true),
    StructField("q0134",IntegerType,true),StructField("q0135",IntegerType,true),StructField("q0136",IntegerType,true),
    StructField("q0137",StringType,true),StructField("q0138",StringType,true),StructField("q0139",StringType,true),
    StructField("q0140",StringType,true),StructField("q0141",StringType,true),StructField("q0170",IntegerType,true),
    StructField("q0171",StringType,true),StructField("q0172",IntegerType,true),StructField("q1000",DoubleType,true),
    StructField("q1001",DoubleType,true),StructField("q1002",DoubleType,true),StructField("q1003",DoubleType,true),
    StructField("q1004",DoubleType,true),StructField("q1005",DoubleType,true),StructField("q1006",DoubleType,true),
    StructField("q1007",DoubleType,true),StructField("q1008",DoubleType,true),StructField("q1009",DoubleType,true),
    StructField("q1010",DoubleType,true),StructField("q1011",DoubleType,true),StructField("q1012",DoubleType,true),
    StructField("q1013",DoubleType,true),StructField("q1014",DoubleType,true),StructField("q1015",DoubleType,true),
    StructField("q1016",DoubleType,true),StructField("q1017",DoubleType,true),
    StructField("q1018",DoubleType,true),StructField("q1019",DoubleType,true),
    StructField("q1020",DoubleType,true),StructField("q1021",DoubleType,true),StructField("q1022",DoubleType,true),
    StructField("q1023",DoubleType,true),StructField("q1024",DoubleType,true),StructField("q1025",DoubleType,true),
    StructField("q1026",DoubleType,true),StructField("q1027",DoubleType,true),StructField("q1028",DoubleType,true),
    StructField("q1029",DoubleType,true),StructField("q1030",DoubleType,true),StructField("q1031",DoubleType,true),
    StructField("q1032",DoubleType,true),StructField("q1033",DoubleType,true),StructField("q1034",DoubleType,true),
    StructField("q1035",DoubleType,true),StructField("q1036",DoubleType,true),StructField("q1037",DoubleType,true),
    StructField("q1038",DoubleType,true),StructField("q1039",DoubleType,true),StructField("q1040",DoubleType,true),
    StructField("q1041",DoubleType,true),StructField("q1042",DoubleType,true),StructField("q1043",DoubleType,true),
    StructField("q1044",DoubleType,true),StructField("q1045",DoubleType,true),StructField("q1046",DoubleType,true),
    StructField("q1047",DoubleType,true),StructField("q1048",DoubleType,true),StructField("q1049",DoubleType,true),
    StructField("q1050",DoubleType,true),StructField("q1051",DoubleType,true),StructField("q1052",DoubleType,true),
    StructField("q1053",DoubleType,true),StructField("q1054",DoubleType,true),StructField("q1055",DoubleType,true),
    StructField("q1056",DoubleType,true),StructField("q1057",DoubleType,true),StructField("q1058",DoubleType,true),
    StructField("q1059",DoubleType,true),StructField("q1060",DoubleType,true),StructField("q1061",DoubleType,true),
    StructField("q1062",DoubleType,true),StructField("q1063",DoubleType,true),StructField("q1064",DoubleType,true),
    StructField("q1065",DoubleType,true),StructField("q1066",DoubleType,true),StructField("q1067",DoubleType,true),
    StructField("q1068",DoubleType,true),StructField("q1069",DoubleType,true),StructField("q1070",DoubleType,true),
    StructField("q1071",DoubleType,true),StructField("q1072",DoubleType,true),StructField("q1073",DoubleType,true),
    StructField("q1074",DoubleType,true),StructField("q1075",DoubleType,true),StructField("q1076",DoubleType,true),
    StructField("q1077",DoubleType,true),StructField("q1078",DoubleType,true),StructField("q1079",DoubleType,true),
    StructField("q1080",DoubleType,true),StructField("q1081",DoubleType,true),StructField("q1082",DoubleType,true),
    StructField("q1083",DoubleType,true),StructField("q1084",DoubleType,true),StructField("q1085",DoubleType,true),
    StructField("q1086",DoubleType,true),StructField("q1087",DoubleType,true),StructField("q1106",DoubleType,true),
    StructField("q1107",DoubleType,true),StructField("q1108",DoubleType,true),StructField("q1109",DoubleType,true),
    StructField("q1110",DoubleType,true),StructField("q1111",DoubleType,true),StructField("q1112",DoubleType,true),
    StructField("q1119",DoubleType,true),StructField("q1120",DoubleType,true),StructField("q1121",DoubleType,true),
    StructField("q1123",DoubleType,true),StructField("q1124",DoubleType,true),
    StructField("q1125",DoubleType,true),StructField("q1126",DoubleType,true),StructField("q1127",DoubleType,true),
    StructField("q1128",DoubleType,true),StructField("q1129",DoubleType,true),StructField("q2000",DoubleType,true),
    StructField("q2001",DoubleType,true),StructField("q2002",DoubleType,true),StructField("q2003",DoubleType,true),
    StructField("q2004",DoubleType,true),StructField("q2005",DoubleType,true),StructField("q2006",DoubleType,true),
    StructField("q2007",DoubleType,true),StructField("q2008",DoubleType,true),StructField("q2009",DoubleType,true),
    StructField("q2010",DoubleType,true),StructField("q2011",DoubleType,true),StructField("q2012",DoubleType,true),
    StructField("q2013",DoubleType,true),StructField("q2014",DoubleType,true),StructField("q2015",DoubleType,true),
    StructField("q2016",DoubleType,true),StructField("q2017",DoubleType,true),StructField("q2018",DoubleType,true),
    StructField("q2019",DoubleType,true),StructField("q2020",DoubleType,true),StructField("q2021",DoubleType,true),
    StructField("q2022",DoubleType,true),StructField("q2023",DoubleType,true),StructField("q2024",DoubleType,true),
    StructField("q2025",DoubleType,true),StructField("q2026",DoubleType,true),StructField("q2027",DoubleType,true),
    StructField("q2028",DoubleType,true),StructField("q2029",DoubleType,true),StructField("q2030",DoubleType,true),
    StructField("q2031",DoubleType,true),StructField("q2032",DoubleType,true),StructField("q2033",DoubleType,true),
    StructField("q2034",DoubleType,true),StructField("q2035",DoubleType,true),StructField("q2036",DoubleType,true),
    StructField("q2037",DoubleType,true),StructField("q2038",DoubleType,true),StructField("q2039",DoubleType,true),
    StructField("q2040",DoubleType,true),StructField("q2041",DoubleType,true),StructField("q2104",DoubleType,true),
    StructField("q2105",DoubleType,true),StructField("q2107",DoubleType,true),StructField("q2108",DoubleType,true),
    StructField("q2111",DoubleType,true),StructField("q2112",DoubleType,true),StructField("q2113",DoubleType,true),
    StructField("q2115",DoubleType,true),StructField("q2116",DoubleType,true),StructField("q2117",DoubleType,true),
    StructField("q2118",DoubleType,true),StructField("q2119",DoubleType,true),StructField("q2134",DoubleType,true),
    StructField("q2135",DoubleType,true),StructField("q2136",DoubleType,true),StructField("q3000",DoubleType,true),
    StructField("q3001",DoubleType,true),StructField("q3002",DoubleType,true),StructField("q3003",DoubleType,true),
    StructField("q3004",DoubleType,true),StructField("q3005",DoubleType,true),StructField("q3006",DoubleType,true),
    StructField("q3007",DoubleType,true),StructField("q3008",DoubleType,true),StructField("q3009",DoubleType,true),
    StructField("q3010",DoubleType,true),StructField("q3011",DoubleType,true),StructField("q3012",DoubleType,true),
    StructField("q3013",DoubleType,true),StructField("q3014",DoubleType,true),StructField("q3015",DoubleType,true),
    StructField("q3016",DoubleType,true),StructField("q3017",DoubleType,true),StructField("q3018",DoubleType,true),
    StructField("q3019",DoubleType,true),StructField("q3020",DoubleType,true),StructField("q3021",DoubleType,true),
    StructField("q3022",DoubleType,true),StructField("q3023",DoubleType,true),StructField("q3024",DoubleType,true),
    StructField("q3025",DoubleType,true),StructField("q3026",DoubleType,true),StructField("q3027",DoubleType,true),
    StructField("q3028",DoubleType,true),StructField("q3029",DoubleType,true),StructField("q3030",DoubleType,true),
    StructField("q3031",DoubleType,true),StructField("q3032",DoubleType,true),StructField("q3033",DoubleType,true),
    StructField("q3034",DoubleType,true),StructField("q3100",DoubleType,true),StructField("q3101",DoubleType,true),
    StructField("q3102",DoubleType,true),StructField("q3103",DoubleType,true),StructField("q3104",DoubleType,true),
    StructField("q3105",DoubleType,true),StructField("q3106",DoubleType,true),StructField("q3107",DoubleType,true),
    StructField("q3108",DoubleType,true),StructField("q3109",DoubleType,true),StructField("q3110",DoubleType,true),
    StructField("q3111",DoubleType,true),StructField("q3112",DoubleType,true),
    StructField("q3113",DoubleType,true),StructField("q3114",DoubleType,true),
    StructField("q3115",DoubleType,true),StructField("q3116",DoubleType,true),StructField("q3117",DoubleType,true),
    StructField("q3118",DoubleType,true),StructField("q9100",StringType,true),StructField("q9101",DoubleType,true),
    StructField("q9102",DoubleType,true),StructField("q9103",DoubleType,true),StructField("q9104",DoubleType,true),
    StructField("q9191",DoubleType,true),StructField("q9192",DoubleType,true),StructField("q9200",StringType,true),
    StructField("q9201",DoubleType,true),StructField("q9202",DoubleType,true),StructField("q9203",DoubleType,true),
    StructField("q9204",DoubleType,true),StructField("q9291",DoubleType,true),StructField("q9292",DoubleType,true)))

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



}
