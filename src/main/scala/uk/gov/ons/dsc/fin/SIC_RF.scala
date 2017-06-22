package uk.gov.ons.dsc.fin

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{Binarizer, CountVectorizer, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode }
import org.apache.spark.sql.Column
//import uk.gov.ons.dsc.fin.SICNaiveBayes.{cvModelName, indexer_label, modelNB, traingEval}

/**
  * Created by noyva on 25/05/2017.
  */
object SIC_RF {

  // features
  val cvModelName  = new Binarizer()
    .setInputCol("q1043")
    .setOutputCol("feature")
    .setThreshold(0.5)

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


  def  main (args:Array[String]):Unit = {

    val appName = "FinBins_PredictSIC_RF"
    //val master = args(0)
    val master = "yarn-client"



    //init
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //Load and Prep data
    val fssIDBR = sqlContext.read.load("fss_idbr")
      .withColumnRenamed("C5","SIC")
      .withColumnRenamed("C26","CompanyName")
      .withColumnRenamed("C32","AddressLine1")
      .withColumnRenamed("q1043","features")
      .dropDuplicates(Array("CompanyName"))

    fssIDBR.registerTempTable("fss_idbr")


    val Array(trainingData, testData) = fssIDBR.randomSplit(Array(0.90, 0.10))
    trainingData.cache.count
    testData.cache.count

    val fssPred =traingEval(Array( indexer_label, modelRF.setFeaturesCol("features")), trainingData, testData, sqlContext)
    fssPred.write.mode(SaveMode.Overwrite).save("predictions")


  }


    def traingEval(stages: Array[PipelineStage], trainingData: DataFrame, testData:DataFrame, sqlContext: SQLContext): DataFrame= {
      val pipeline = new Pipeline()
        .setStages(stages)

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      predictions.write.mode(SaveMode.Overwrite).save("predictions_RF_raw")

      println("predictions_RF_raw saved ...")

      predictions.registerTempTable("predictions")

      sqlContext.sql("select SIC, label, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  SIC, label, prediction order by SIC, prediction asc ").repartition((1))

    }


}
