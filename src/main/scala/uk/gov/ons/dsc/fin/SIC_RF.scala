package uk.gov.ons.dsc.fin

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import uk.gov.ons.dsc.fin.SICNaiveBayes.{cvModelName, indexer_label, modelNB, traingEval}

/**
  * Created by noyva on 25/05/2017.
  */
object SIC_RF {

  // features
  val cvModelName  = new CountVectorizer()
    .setInputCol("ngram_name")
    .setOutputCol("name_features")
    .setMinDF(2)

  // labels
  val indexer_label = new StringIndexer()
    .setInputCol("SIC")
    .setOutputCol("label")


  //model
  val modelRF = new RandomForestClassifier()
                              .setNumTrees(5)
                              .setFeatureSubsetStrategy("auto")
                              .setImpurity("gini")
                              .setMaxDepth(4)
                              .setMaxBins(32)


  def  main (args:Array[String]):Unit = {

    val appName = "FinBins_PredictSIC_RF"
    //val master = args(0)
    val master = "yarn-client"
    var ngram_size = 4
    if (args.length > 0) {
      ngram_size = args(1).toInt
    }


    //init
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    //Load and Prep data
    val fssIDBR = sqlContext.read.load("fss_idbr")
      .withColumnRenamed("C5","SIC")
      .withColumnRenamed("C26","CompanyName")
      .withColumnRenamed("C32","AddressLine1")
      .dropDuplicates(Array("CompanyName"))

    fssIDBR.registerTempTable("fss_idbr")


    val Array(trainingData, testData) = fssIDBR.randomSplit(Array(0.90, 0.10))
    trainingData.cache.count
    testData.cache.count

    val fssPred =traingEval(Array(cvModelName, indexer_label, modelRF.setFeaturesCol(cvModelName.getOutputCol)), trainingData, testData, sqlContext)
    fssPred.write.mode(SaveMode.Overwrite).save("predictions")


  }


    def traingEval(stages: Array[PipelineStage], trainingData: DataFrame, testData:DataFrame, sqlContext: SQLContext): DataFrame= {
      val pipeline = new Pipeline()
        .setStages(stages)

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      predictions.write.mode(SaveMode.Overwrite).save("predictions_raw")

      predictions.registerTempTable("predictions")

      sqlContext.sql("select SIC, label, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  SIC, label, prediction order by SIC, prediction asc ").repartition((1))

    }


}
