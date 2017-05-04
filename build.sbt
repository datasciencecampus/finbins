name := "FinBins"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.0"


//libraryDependencies += "com.databricks" %% "spark-csv_2.10" % "1.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
        