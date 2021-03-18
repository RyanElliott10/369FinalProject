package WSBSentiment

import scala.collection._
import scala.io._
import scala.math.BigDecimal.double2bigDecimal
import scala.math._
import scala.util.Random

import org.apache.log4j.{ Logger, Level }
import org.apache.spark.SparkContext._
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier,
  NaiveBayes
}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{
  HashingTF,
  IDF,
  Tokenizer,
  StopWordsRemover,
  Word2Vec,
  NGram
}
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }

import co.theasi.plotly._
import co.theasi.plotly.{Plot, draw, writer}

package object consts {
  val minCommentLen = 10
  val maxTokenLen = 15
  val sentimentThreshold = 0.5
  val negativeLabel = 0
  val positiveLabel = 1
  val neutralLabel = 2
  val numTrees = 32
}

object SentimentType extends Enumeration {
  type SentimentType = Value;
  val Negative, Neutral, Positive = Value;

  case class SentimentTypeValue(sentiment: Value) {
    def findBinContinuous(compound: Float): Value = {
      compound match {
        case x if (-1 <= x && x <= -consts.sentimentThreshold) => Negative
        case x if (-consts.sentimentThreshold < x && x <= consts.sentimentThreshold) => Neutral
        case x if (consts.sentimentThreshold < x && x <= 1) => Positive
      }
    }
  }

  implicit def typeToSentimentType(sent: Value) = new SentimentTypeValue(sent)
}

import SentimentType._

case class SentimentComment(body: String, neg: Float, neu: Float,
                            pos: Float, compound: Float, bin: SentimentType)

object App {
  def cleanString(someStr: String): String = {
    someStr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WSBSentiment").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // The current filter removes neutral sentiment; only predicts positive and negative sentiment
    val rdd: RDD[(Int, String)] = sc.textFile(args(0)).map{line =>
      val sp = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      sp.length match {
        case 8 => {
          if (sp(1)(0) == '{') {
            val doc = sp(0)
              .replaceAll("[^A-Za-z0-9 ]", "")
              .split(" ")
              .filter(_.length <= consts.maxTokenLen)
              .mkString(" ")
            val compound = sp(6).trim.toDouble match {
              case x if (-1 <= x && x <= -consts.sentimentThreshold) => consts.negativeLabel
              case x if (consts.sentimentThreshold < x && x <= 1) => consts.positiveLabel
              case _ => consts.neutralLabel
            }
            (compound, doc)
          } else {
            null
          }
        }
        case _ => null
      }
    }.filter{line =>
      line != null && line._2.length > consts.minCommentLen && line._1 != consts.neutralLabel
    }

    val vocabSize = rdd.flatMap(_._2).collect.toSet.size

    val tokenizer = new Tokenizer()
      .setInputCol("comment")
      .setOutputCol("tokens")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("tokensNoStop")

    val hashingTF = new HashingTF()
      .setInputCol("tokensNoStop")
      .setOutputCol("rawFeatures")
      .setNumFeatures(vocabSize)
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val ngram = new NGram()
      .setInputCol("tokens")
      .setOutputCol("bigrams")
      .setN(2)

    val word2vec = new Word2Vec()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setVectorSize(256)
      .setWindowSize(5)
      .setMinCount(0)

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(consts.numTrees)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, word2vec, rf))

    val commentData = spark.createDataFrame(rdd).toDF("label", "comment")
    val Array(trainSet, testSet) = commentData.randomSplit(Array(0.8, 0.2), seed=42)

    val model = pipeline.fit(trainSet)
    val predictions = model.transform(testSet)

    predictions.select("prediction", "label", "comment").show(20, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    val testError = 1.0 - accuracy
    println(s"Model Error: ${round(testError*100)}%")
    model.write.overwrite.save("models/random_forest.model")

    // Financial data
    val cwd = new java.io.File(".").getCanonicalPath
    val stockTSPATH = cwd + "/src/main/python/stockData/"

    val stockTS = sc.textFile(stockTSPATH + "output.csv")
    val header = stockTS.first()
    val data = stockTS.filter(_ != header)
      .map(x => {
        val temp = x.split(",")
        (temp(1), (temp(2).toDouble, temp(3))) // (date, (price, close))
      })
      .filter(x => x._2._2 != "open")

    val start = data.take(1)(0)._2._1

    // add in % change for point labels
    val dataNew = data.map(x => {
      val diff = 10.0*(x._2._1/start - 1).setScale(4, BigDecimal.RoundingMode.HALF_UP)
      (x._1 + " : " + diff.toString() + "%", x._2 )
    })


    val ytest = data.map(x => x._2._1).collect
    val xtest = (1 to (ytest.length-1).toInt).toList
    val pointLabels = dataNew.map(x => x._1).collect

    val commonAxisOptions = AxisOptions()
    val commonOptions = ScatterOptions()
      .mode(ScatterMode.Line)

    // add in time and ticker args here
    val labels = sc.textFile(stockTSPATH + "descriptor.csv").first().split(",")
    val xAxisOptions = commonAxisOptions.title("past " + labels(1))
    val yAxisOptions = commonAxisOptions.title(labels(0) + " price (USD)")

    val p = Plot()
      .withScatter(xtest, ytest, commonOptions.name("Date").text(pointLabels))
      .xAxisOptions(xAxisOptions)
      .yAxisOptions(yAxisOptions)

    val figure = Figure()
      .plot(p)
      .title("AMRN 1 month price history")

    draw(p, "basic-scatter", writer.FileOptions(overwrite=true))
  }
}
