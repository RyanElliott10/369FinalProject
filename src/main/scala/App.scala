package WSBSentiment

import scala.io._
import scala.math._
import scala.collection._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.{ Logger, Level }
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
  StopWordsRemover
}
import org.apache.spark.ml.{ Pipeline, PipelineModel }

package object consts {
  val minCommentLen = 25
  val maxTokenLen = 15
}

object SentimentType extends Enumeration {
  type SentimentType = Value;
  val Negative, Neutral, Positive = Value;

  case class SentimentTypeValue(sentiment: Value) {
    def findBinContinuous(compound: Float): Value = {
      compound match {
        case x if (-1 <= x && x <= -0.33) => Negative
        case x if (-0.33 < x && x <= 0.33) => Neutral
        case x if (0.33 < x && x <= 1) => Positive
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

    // Dictionary source from local
    val englishDict = scala.io.Source.fromFile("/usr/share/dict/web2").getLines.toSet

    val rdd: RDD[(Int, String)] = sc.textFile(args(0)).map{line =>
      val sp = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      sp.length match {
        case 8 => {
          if (sp(1)(0) == '{') {
            val doc = sp(0)
              .replaceAll("[^A-Za-z0-9 ]", "")
              .split(" ")
              .filter(tok => englishDict.contains(tok))
              .mkString(" ")
            val compound = sp(6).trim.toDouble match {
              case x if (-1 <= x && x <= -0.5) => 0
              case x if (0.5 < x && x <= 1) => 1
              case _ => 2
            }
            (compound, doc)
          } else {
            null
          }
        }
        case _ => null
      }
    }.filter{line =>
      line != null && line._2.length > consts.minCommentLen
    }

    val vocabSize = rdd.flatMap(_._2).collect.toSet.size
    var bestModel: (Double, PipelineModel, Int) = (1.0, null, 0)

    (32 to 256 by 32).foreach{trees =>
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
      val rf = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setNumTrees(trees)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, rf))

      val commentData = spark.createDataFrame(rdd).toDF("label", "comment")
      val Array(trainSet, testSet) = commentData.randomSplit(Array(0.8, 0.2), seed=42)

      val model = pipeline.fit(trainSet)
      val predictions = model.transform(testSet)

      predictions.select("prediction", "label", "comment").show()

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)

      val testError = 1.0 - accuracy
      if (testError < (bestModel._1)) {
        println("New Best Model")
        bestModel = (testError, model, trees)
      }
      println(s"${trees} - Test Error = ${testError}")
    }

    println(s"Best Model: ${bestModel._3} Trees, ${round(bestModel._1*100)}% Error")
    bestModel._2.write.overwrite.save("models/random_forest.model")
  }
}
