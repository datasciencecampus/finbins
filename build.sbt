

lazy val root = (project in file(".")).
      settings (
        name := "FinBins",
        version := "2.0",
        scalaVersion := "2.11.8",
        mainClass in Compile := Some ("uk.gov.ons.dsc.fin.FinBins")


      )

/*libraryDependencies += "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4" */

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided" ,
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided" ,
  "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
)


//META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

        