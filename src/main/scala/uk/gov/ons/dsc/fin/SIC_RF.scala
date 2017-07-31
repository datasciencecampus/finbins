package uk.gov.ons.dsc.fin

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.types._

//import uk.gov.ons.dsc.fin.SICNaiveBayes.{cvModelName, indexer_label, modelNB, traingEval}

/**
  * Created by noyva on 25/05/2017.
  */
object SIC_RF {

  // features
  val assembler = new VectorAssembler()
    .setOutputCol("features")

  // labels
  val indexer_label = new StringIndexer()
    .setInputCol("SIC")
    .setOutputCol("label")


  //model
  val modelRF = new RandomForestClassifier()
                              .setNumTrees(7)
                              .setFeatureSubsetStrategy("auto")
                              .setImpurity("gini")
                              .setMaxDepth(4)
                              .setMaxBins(32)
                           //   .setFeaturesCol("features")

  //result schema
  val resSchema = StructType(
    Array(
      StructField("features", StringType),
      StructField("accuracy", DoubleType)
    )
  )


  def  main(args:Array[String]):Unit = {

    val appName = "FinBins_PredictSIC_RF_restricted"

    val numFeatures = args(0).toInt // number of features in the RF classifier
    val startPos    = args(1).toInt    // starting combination
    val endComb     = args(2).toInt   // end combination

    val master = "yarn-client"



    //init Session
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("FinBins-SIC_RF")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    //Load and Prep data
    val fssIDBR = spark.read.load("fss_idbr")
      .withColumnRenamed("C5","SIC")
      .withColumnRenamed("C26","CompanyName")
      .withColumnRenamed("C32","AddressLine1")
      // .withColumn("features")
      //   .withColumn("features",toVec4(fssIDBR(""),fssIDBR("")))
      .dropDuplicates(Array("CompanyName"))
    .na.fill(0)

    val featureCols = fssIDBR.dtypes.filter(f=>  f._2=="DoubleType").map(f=>f._1)

    val featuresCombIter = featureCols.combinations(numFeatures)

    var counter:Long = 0

    fssIDBR.registerTempTable("fss_idbr")


    val Array(trainingData, testData) = fssIDBR.randomSplit(Array(0.90, 0.10))
    trainingData.cache.count
    testData.cache.count

    var output = scala.collection.mutable.ListBuffer.empty[Row]

    var fCols:Array[String] = null

    while (featuresCombIter.hasNext && counter <= endComb) {

      fCols = featuresCombIter.next()

      if (counter >= startPos) {
        assembler.setInputCols(fCols)

        val fssPred = traingEval(Array(assembler, indexer_label, modelRF.setFeaturesCol("features")), trainingData, testData, spark.sqlContext)

        val accuracy: Double = fssPred.select(avg((col("numCorrect") / col("total")))).first().getDouble(0)

        fssPred.write.mode("overwrite").json("RF_SIC_results/resRF_"+fCols.mkString("_")+".json")

        println("No:" + counter.toString + " accuracy for features:" + fCols.mkString(",") + " is:" + accuracy)

        output = output :+ Row(fCols.mkString(","), accuracy)
      }
      counter += 1


    }

    val parallelizedRows = spark.sparkContext.parallelize(output.toSeq)

    val resultDF = spark.createDataFrame(parallelizedRows,resSchema)

    resultDF.write.mode(SaveMode.Overwrite).save("SIC_predictions_feature_sel_"+numFeatures.toString)

    println("Number of feature combinations saved:"+resultDF.count())
    resultDF.show (100)


  }


    def traingEval(stages: Array[PipelineStage], trainingData: DataFrame, testData:DataFrame, sqlContext: SQLContext): DataFrame= {
      val pipeline = new Pipeline()
        .setStages(stages)

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      // predictions.write.mode(SaveMode.Overwrite).save("predictions_RF_raw")

      println("predictions_made ...")

      predictions.createOrReplaceTempView("predictions")

      sqlContext.sql("select SIC, label, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  SIC, label, prediction order by SIC, prediction asc ").repartition((1))

    }




}
