package uk.gov.ons.dsc.fin

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by noyva on 09/06/2017.
  */
object kNN {



  def main (args:Array[String]):Unit = {

    val appName = "FinBins_kNN"
    //val master = args(0)
    val master = "yarn-client"


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fss_idbr = sqlContext.read.load("fss_idbr")

    //register table
    fss_idbr.registerTempTable("fss_idbr")
    fss_idbr.registerTempTable("fss1")

    // find the maximum values of the columns
    val fss_idbr_max = sqlContext.sql("select max(q1001) as q1001m,max(q1002) as q1002m,max(q1003) as q1003m,max(q1004) as q1004m,max(q1005) as q1005m,max(q1006) as q1006m,max(q1007) as q1007m,max(q1008) as q1008m,max(q1009) as q1009m,max(q1010) as q1010m," +
      "max(q1011) as q1011m,max(q1012) as q1012m,max(q1013) as q1013m,max(q1014) as q1014m,max(q1015) as q1015m,max(q1016) as q1016m,max(q1017) as q1017m,max(q1018) as q1018m,max(q1019) as q1019m,max(q1020) as q1020m," +
      "max(q1023) as q1023m,max(q1024) as q1024m,max(q1025) as q1025m,max(q1026) as q1026m,max(q1027) as q1027m,max(q1028) as q1028m,max(q1029) as q1029m,max(q1030) as q1030m,max(q1031) as q1031m,max(q1032) as q1032m,max(q1033) as q1033m,max(q1034) as q1034m," +
      "max(q1035) as q1035m,max(q1036) as q1036m,max(q1037) as q1037m,max(q1038) as q1038m,max(q1039) as q1039m,max(q1040) as q1040m,max(q1041) as q1041m,max(q1042) as q1042m,max(q1043) as q1043m,max(q1044) as q1044m,max(q1045) as q1045m," +
      "max(q1046) as q1046m,max(q1047) as q1047m,max(q1048) as q1048m,max(q1049) as q1049m,max(q1050) as q1050m,max(q1051) as q1051m,max(q1052) as q1052m,max(q1053) as q1053m,max(q1054) as q1054m,max(q1055) as q1055m,max(q1056) as q1056m," +
      "max(q1057) as q1057m,max(q1058) as q1058m,max(q1059) as q1059m,max(q1060) as q1060m,max(q1061) as q1061m,max(q1062) as q1062m,max(q1063) as q1063m,max(q1064) as q1064m,max(q1065) as q1065m,max(q1066) as q1066m,max(q1067) as q1067m," +
      "max(q1068) as q1068m,max(q1069) as q1069m,max(q1070) as q1070m,max(q1071) as q1071m,max(q1072) as q1072m,max(q1073) as q1073m,max(q1074) as q1074m,max(q1075) as q1075m,max(q1076) as q1076m,max(q1077) as q1077m,max(q1078) as q1078m," +
      "max(q1079) as q1079m,max(q1080) as q1080m,max(q1081) as q1081m,max(q1082) as q1082m,max(q1083) as q1083m,max(q1084) as q1084m,max(q1085) as q1085m,max(q1086) as q1086m,max(q1087) as q1087m,max(q1106) as q1106m," +
      "max(q1107) as q1107m,max(q1108) as q1108m,max(q1109) as q1109m,max(q1110) as q1110m,max(q1111) as q1111m,max(q1112) as q1112m,max(q1119) as q1119m,max(q1120) as q1120m,max(q1121) as q1121m,max(q1123) as q1123m,max(q1124) as q1124m," +
      "max(q1125) as q1125m,max(q1126) as q1126m,max(q1127) as q1127m,max(q1128) as q1128m,max(q1129) as q1129m,max(q2000) as q2000m,max(q2001) as q2001m,max(q2002) as q2002m,max(q2003) as q2003m,max(q2004) as q2004m,max(q2005) as q2005m," +
      "max(q2006) as q2006m,max(q2007) as q2007m,max(q2008) as q2008m,max(q2009) as q2009m,max(q2010) as q2010m,max(q2011) as q2011m,max(q2012) as q2012m,max(q2013) as q2013m,max(q2014) as q2014m,max(q2015) as q2015m,max(q2016) as q2016m," +
      "max(q2017) as q2017m,max(q2018) as q2018m,max(q2019) as q2019m,max(q2020) as q2020m,max(q2021) as q2021m,max(q2022) as q2022m,max(q2023) as q2023m,max(q2024) as q2024m,max(q2025) as q2025m     from fss_idbr")

    //display the results
    fss_idbr_max.show(1,false)

    //save the max values
    fss_idbr_max.write.mode(SaveMode.Overwrite).save("fss_idbr_max")

   // val fss_idbr_norm = sqlContext.sql("")

    // normalise
    //val fss_idbr_norm = sqlContext.sql("")

    //val fss_idbr_dist = fss_idbr.withColumn("D1",null)


    // calculate combinations

    val distances = sqlContext.sql("select fss1.RUReference AS Ref1, fss_idbr.RUReference AS Ref2, SQRT (POWER((fss1.q1001 - fss_idbr.q1001),2) + POWER((fss1.q1002 - fss_idbr.q1002),2)) as dist from fss_idbr,fss1 where fss1.RUReference > fss_idbr.RUReference  ")

    distances.write.mode(SaveMode.Overwrite).save("distances")

    println("distances No:"+distances.count)

    // assign rows to clusters
  }

}
