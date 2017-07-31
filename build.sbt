

lazy val root = (project in file(".")).
      settings (
        name := "FinBins",
        version := "2.0",
        scalaVersion := "2.11.8",
        mainClass in Compile := Some ("uk.gov.ons.dsc.fin.FinBins")


      )

/*libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4" */

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0"  ,
  "org.apache.spark" %% "spark-sql" % "2.1.0"  ,
  "org.apache.spark" %% "spark-mllib" % "2.1.0"
)

/*
//META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF",  xs @ _*)                => MergeStrategy.discard
  case PathList(ps @ _*)                             => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x                                             => MergeStrategy.first
 //   val oldStrategy = (assemblyMergeStrategy in assembly).value
 //   oldStrategy(x)


}

*/
        