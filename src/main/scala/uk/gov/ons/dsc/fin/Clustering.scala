package uk.gov.ons.dsc.fin

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, SparkSession}

object Clustering {

  // features
  val assembler = new VectorAssembler()
    // .setInputCols(Array("q1043", "q1044"))
    .setOutputCol("features")




  //model
  val modelKMeans =  new KMeans()
                    .setK(5)
                    .setSeed(1L)
                    .setPredictionCol("clusterNo")



  def  main(args:Array[String]):Unit = {

    val appName = "FinBins_Clustering"
    val numFeatures = args.length

    if (numFeatures <= 0) {
      println("Not enoiugh arguments. Exiting ....")
      sys.exit()
    }

    val fCols = args

    println("usage: feature1 feature2 feature3 ....")
    println("Running with features:" + fCols.mkString(","))


    //init Session
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("FinBins-Clustering")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // configure the feature columns in the assembler
    assembler.setInputCols(fCols)
    println("setting the features to:" + fCols.mkString(","))


    //Load and Prep data
    val fssIDBRraw = spark.read.load("fss_idbr")
      .withColumnRenamed("frosic2007", "SIC")
      //.withColumn("Sub_SIC", substrSIC (col("SIC")))
      // .dropDuplicates(Array("CompanyName"))
      .na.fill(0)
    //  .filter("frosic2007 < 66666")


    val fssIDBR = assembler.transform(fssIDBRraw).cache
    val model = modelKMeans.fit(fssIDBR)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(fssIDBR)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    //save the dataset
    fssIDBR.write.mode(SaveMode.Overwrite).save("KMeansResults")

    println("KMeansResults resulyts saved")


  }

}
