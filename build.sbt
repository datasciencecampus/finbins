import sun.security.tools.PathList

lazy val root = (project in file(".")).
      settings (
        name := "FinBins",
        version := "1.0",
        scalaVersion := "2.10.5",
        mainClass in Compile := Some ("gov.uk.ons.DSC.Fin.FinBins")


      )

//libraryDependencies += "com.databricks" %% "spark-csv_2.10" % "1.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided" ,
  "org.apache.spark" %% "spark-mllib" % "1.6.0" % "provided"
)


//META-INF discarding
/*
mergeStrategy in assembly <<= (mergeStrategy in assembly) {(old) =>
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }

}

*/
        