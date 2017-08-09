package uk.gov.ons.dsc.fin

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

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
    .setInputCol("Sub_SIC")
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
    val numFeatures = args.length - 1

    if (numFeatures <= 0) {
      println("Not enoiugh arguments. Exiting ....")
      sys.exit()
    }

    val SICchars = try {args(0).toInt} catch {case e:Exception => println ("exception parcing number of SIC chars used in SIC code: " + e); 5}
    val fCols = args.drop(1)
    println("usage: NoSICChars feature1 feature2 feature3 ....")
    println("Running with SIC"+SICchars + " and features:"+ fCols.mkString(",") )



    //init Session
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("FinBins-SIC_RF_SINGLE")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    def subsringFn (str:String) = {
      str.take(SICchars)
    }

    //define UDF for SIC truncating
    val substrSIC = udf (subsringFn _)


    //Load and Prep data
    val fssIDBR = spark.read.load("fss_idbr")
      .withColumnRenamed("C5","SIC")
      .withColumnRenamed("C26","CompanyName")
      .withColumnRenamed("C32","AddressLine1")
      .withColumn("Sub_SIC", substrSIC (col("SIC")))
      // .withColumn("features")
      //   .withColumn("features",toVec4(fssIDBR(""),fssIDBR("")))
      .dropDuplicates(Array("CompanyName"))
      .na.fill(0)

    fssIDBR.createOrReplaceTempView("fss_idbr")
    val Array(trainingData, testData) = fssIDBR.randomSplit(Array(0.90, 0.10))
    trainingData.cache.count
    testData.cache.count

    // configure the feature columns in the assembler
    assembler.setInputCols(fCols)
    println("setting the features to:"+fCols.mkString(","))



    val fssPred =traingEval(Array( assembler,indexer_label, modelRF.setFeaturesCol("features")), trainingData, testData, spark.sqlContext)
    fssPred.write.mode(SaveMode.Overwrite).save("SIC_predictionsFrom"+fCols.mkString("_"))

    fssPred.write.mode("overwrite").json("RF_SIC_results/resRF_SIC_"+SICchars+"_" +fCols.mkString("_")+".json")

    fssPred.createOrReplaceTempView("pred")

    println ("Results for features:"+ fCols.mkString(",") )
    fssPred.show(50)

    println ("Results for features:"+ fCols.mkString(",") )


    val accuracy: Double = spark.sql("select sum(numCorrect)/sum(total) as accuracy from pred").first.getDouble(0)

    println( " Accuracy for features:" + fCols.mkString(",") + " is:" + accuracy)

  }


  def traingEval(stages: Array[PipelineStage], trainingData: DataFrame, testData:DataFrame, sqlContext: SQLContext): DataFrame= {
    val pipeline = new Pipeline()
      .setStages(stages)

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.write.mode(SaveMode.Overwrite).save("predictions_RF_raw")

    println("predictions_RF_raw saved ...")

    predictions.createOrReplaceTempView("predictions")

    sqlContext.sql("select Sub_SIC, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  Sub_SIC order by Sub_SIC ").repartition((1))
    //sqlContext.sql("select Sub_SIC, label, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  Sub_SIC, label, prediction order by Sub_SIC, prediction asc ").repartition((1))

  }


}
