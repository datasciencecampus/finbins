package uk.gov.ons.dsc.fin

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{Binarizer, CountVectorizer, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by noyva on 11/07/2017.
  */
object SIC_RF_SINGL {

  // features
  val assembler = new VectorAssembler()
   // .setInputCols(Array("q1043", "q1044"))
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



  def  main(args:Array[String]):Unit = {

    val appName = "FinBins_PredictSIC_RF_singleModel"
    val numFeatures = args.length

    /*
    val fCols:Array[String] = numFeatures match {
      case 1 => Array(args(0))
      case 2 => Array(args(0), args(1))
      case 3 => Array(args(0), args(1), args(2))
      case 4 => Array(args(0), args(1), args(2), args(3))
      case 5 => Array(args(0), args(1), args(2), args(3), args(4))
      case 5 => Array(args(0), args(1), args(2), args(3), args(4), args(5) )

    }
    */

    val fCols = args

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
      // .withColumn("features")
      //   .withColumn("features",toVec4(fssIDBR(""),fssIDBR("")))
      .dropDuplicates(Array("CompanyName"))
      .na.fill(0)

    fssIDBR.registerTempTable("fss_idbr")


    val Array(trainingData, testData) = fssIDBR.randomSplit(Array(0.90, 0.10))
    trainingData.cache.count
    testData.cache.count

    // configure the feature columns in the assembler
    assembler.setInputCols(fCols)
    println("setting the features to:"+fCols.mkString(","))



    val fssPred =traingEval(Array( assembler,indexer_label, modelRF.setFeaturesCol("features")), trainingData, testData, sqlContext)
    fssPred.write.mode(SaveMode.Overwrite).save("SIC_predictionsFrom"+fCols.mkString("_"))

    fssPred.write.mode("overwrite").json("RF_SIC_results/resRF_"+fCols.mkString("_")+".json")


    println ("Results for features:"+ fCols.mkString(",") )
    fssPred.show(50)

    println ("Results for features:"+ fCols.mkString(",") )

    val accuracy: Double = fssPred.select(avg((col("numCorrect") / col("total")))).map { row => row.getDouble(0) }.first()

    println( " Accuracy for features:" + fCols.mkString(",") + " is:" + accuracy)

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
