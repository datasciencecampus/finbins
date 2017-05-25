package uk.gov.ons.dsc.fin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.NaiveBayes
import uk.gov.ons.dsc.utils.stringmetric.StringMetric
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.IntegerType


/**
  * Created by noyva on 17/05/2017.
  */
object SICNaiveBayes {

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
  val modelNB = new NaiveBayes()

  def main (args:Array[String]):Unit = {

    val appName = "FinBins_PredictSIC_NaiveBayes"
    //val master = args(0)
    val master = "yarn-client"
    var ngram_size = 4
    if (args.length > 0) {
       ngram_size = args(1).toInt
    }



    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    def split(a: String): Array[String] = {
      val ngrams: Seq[Int] = List(ngram_size)
      if (a == null) {
        Array[String]()
      } else {
        val g = ngrams.flatMap(l =>
          a.sliding(l)
        )
        g.toSet.toArray
      }
    }

    def split1(a: String): Array[String] = {
      val ngrams: Seq[Int] = List(ngram_size)
      if (a == null) {
        Array[String]()
      } else {
        a.split (" ")
      }
    }


    sqlContext.udf.register("split", split1 _)

    //Load and Prep data
    val firms_IDBR = sqlContext.read.load("idbr0")
                                    .withColumnRenamed("C5","SIC")
                                    .withColumnRenamed("C26","CompanyName")
                                    .withColumnRenamed("C32","AddressLine1")
                                    .dropDuplicates(Array("CompanyName"))

    firms_IDBR.registerTempTable("firms_IDBR")

    /*
    val divisions = sqlContext.read.load ("sic_divisions")
      .withColumnRenamed("_c0", "sector")
      .withColumnRenamed("_c1", "division")
      .withColumnRenamed("_c2", "description").select("sector", "division", "description")
    divisions.registerTempTable("divisions")
   */

    //sqlContext.sql("select * from companies join divisions on substring(SICCode[0],0,2) =division").registerTempTable("company_divs")

    //val ngrams = sqlContext.sql("select CompanyName,CompanyNumber, split(CompanyName) as ngram_name , split(RegAddress_AddressLine1) as ngram_address , description, sector, division from company_divs").cache


    val ngrams = sqlContext.sql("select CompanyName, split(CompanyName) as ngram_name , split(AddressLine1) as ngram_address , SIC from firms_IDBR").cache


    ngrams.write.mode(SaveMode.Overwrite).save("ngram")

    ngrams.registerTempTable("ngrams")

    val Array(trainingData, testData) = ngrams.randomSplit(Array(0.90, 0.10))
    trainingData.cache.count
    testData.cache.count

    val namePred =traingEval(Array(cvModelName, indexer_label, modelNB.setFeaturesCol(cvModelName.getOutputCol)), trainingData, testData, sqlContext)
    namePred.write.mode(SaveMode.Overwrite).save("predictions")

    namePred.registerTempTable("namepred")

    // print the total accuracy

    println(" Overall accuracy")
    namePred.select(avg( (col("numCorrect") / col("total")))).show

    println(" Results by SICs")
    val accuracyBySIC = sqlContext.sql("select SIC , sum(numCorrect) as Correct, sum(total) as Total, sum(numCorrect)/sum(total) as Accuracy from namepred group by SIC order by Accuracy desc")
    accuracyBySIC.write.mode(SaveMode.Overwrite).save("accuracyBySIC")
    accuracyBySIC.show
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
