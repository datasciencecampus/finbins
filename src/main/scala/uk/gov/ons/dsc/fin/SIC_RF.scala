package uk.gov.ons.dsc.fin

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
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
    .setInputCol("Sub_SIC")
    .setOutputCol("label")
    .setHandleInvalid("skip")




  //model
  val modelRF = new RandomForestClassifier()
                              .setNumTrees(7)
                              .setFeatureSubsetStrategy("auto")
                              .setImpurity("gini")
                              .setMaxDepth(4)
                              .setMaxBins(32)


  //result schema
  val resSchema = StructType(
    Array(
      StructField("features", StringType),
      StructField("accuracy", DoubleType)
    )
  )


  def  main(args:Array[String]):Unit = {

   println("Agruments format is: numFeatures StartPos endPos SIC_chars startCol")


    val numFeatures = try {args(0).toInt} catch {case e:Exception => println ("exception parcing number of features: " + e); 2}  // number of features in the RF classifier
    val startPos    = try {args(1).toInt} catch {case e:Exception => println ("exception parcing starting combination: " + e); 0} // starting combination
    val endComb     = try {args(2).toInt} catch {case e:Exception => println ("exception parcing end combination: " + e); 2500}   // end combination
    val SICchars    = try {args(3).toInt} catch {case e:Exception => println ("exception parcing number of chars used in SIC code: " + e); 5}   // number of chars used in SIC code - max is 5
    val startCol    = try {args(4)} catch {case e:Exception => println ("exception parcing start col: " + e); "A"}



  println("Running with: numFeatures:"+numFeatures+ " StartPos: "+startPos+ " endPos:"+endComb+ " SIC_chars:"+SICchars + " Start col:"+ startCol )

    //init Session
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("FinBins-SIC_RF")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()



  def subsringFn (str:String) = {
    str.take(SICchars)
  }

    //define UDF for SIC truncating
    val substrSIC = udf (subsringFn _)

    //Load and Prep data
    val fssIDBR = spark.read.load("fss_idbr")
      .withColumnRenamed("frosic2007","SIC")
      .withColumn("Sub_SIC", substrSIC (col("SIC")))
      //   .withColumn("features",toVec4(fssIDBR(""),fssIDBR("")))
      //.dropDuplicates(Array("CompanyName"))
      .na.fill(0)
      .filter("frosic2007 < 66666")

    val featureCols = fssIDBR.dtypes.filter(f=>  f._2=="DoubleType").map(f=>f._1).filter(f=>f>=startCol )

    val featuresCombIter = featureCols.combinations(numFeatures)

    //println("Number of combinations:"+featuresCombIter.size)

    var counter:Long = 0

    fssIDBR.createOrReplaceTempView("fss_idbr")


    val Array(trainingData, testData) = fssIDBR.randomSplit(Array(0.90, 0.10))
    trainingData.cache
    testData.cache

    var output = scala.collection.mutable.ListBuffer.empty[Row]



    while (featuresCombIter.hasNext  && counter < endComb) {

      val fCols:Array[String]  = featuresCombIter.next()

      if (counter >= startPos && fCols != null && !fCols.isEmpty) {
        println ("Processing:"+fCols.mkString(","))

        assembler.setInputCols(fCols)
        try {
          //println("t1")
          val fssPred = traingEval(Array(assembler, indexer_label, modelRF.setFeaturesCol("features")), trainingData, testData, spark.sqlContext)
          fssPred.createOrReplaceTempView("pred")
          val accuracy: Double = spark.sql("select sum(numCorrect)/sum(total) as accuracy from pred").first.getDouble(0)

          //println("t3")
          fssPred.write.mode("overwrite").json("RF_SIC_results/resRF_SIC_" + SICchars + "_" + fCols.mkString("_") + ".json")
          //println("t4")
          println("No:" + counter.toString + " accuracy for features:" + fCols.mkString(",") + " is:" + accuracy)

          output = output :+ Row(fCols.mkString(","), accuracy)
        }
         catch {
           case e:Exception => println("exception caught: "+e);
         }
      }
      counter += 1


    }

    val parallelizedRows = spark.sparkContext.parallelize(output.toSeq)

    val resultDF = spark.createDataFrame(parallelizedRows,resSchema).sort(col("accuracy").desc)

    // save the resulting table
    resultDF.write.mode(SaveMode.Overwrite).save("SIC_predictions_feature_sel_"+numFeatures.toString+"_SIC"+SICchars)

    println("All done! Number of feature combinations saved:"+resultDF.count())
    resultDF.show (100)


  }


    def traingEval(stages: Array[PipelineStage], trainingData: DataFrame, testData:DataFrame, sqlContext: SQLContext): DataFrame= {
      val pipeline = new Pipeline()
                        .setStages(stages)
      // println("t1")
      val model = pipeline.fit(trainingData)
      // println("t2")
      val predictions = model.transform(testData)
      // println("t3")
      // predictions.write.mode(SaveMode.Overwrite).save("predictions_RF_raw")

      println("predictions_made  ...")

      predictions.createOrReplaceTempView("predictions")

      sqlContext.sql("select Sub_SIC, count(case when label = prediction then 1 end) as numCorrect,count(case when label <> prediction then 1 end) as numIncorrect, count(*) as total from predictions group by  Sub_SIC order by Sub_SIC ").repartition((1))

      //sqlContext.sql("select Sub_SIC, count(case when label = prediction then 1 end) as numCorrect,count(case when label <> prediction then 1 end) as numIncorrect, count(*) as total from predictions group by  Sub_SIC order by Sub_SIC ").repartition((1))

      //sqlContext.sql("select Sub_SIC, label, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  Sub_SIC, label, prediction order by Sub_SIC, prediction asc ").repartition((1))

      // sqlContext.sql("select SIC, label, count(case when label = prediction then 1 end) as numCorrect, count(*) as total from predictions group by  SIC, label, prediction order by SIC, prediction asc ").repartition((1))

    }




}
