package WSBSentiment

import scala.io._
import scala.collection._
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }

package object consts {
  val minCommentLen = 50
  val maxWordLen = 15
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

case class SentimentComment(var body: String, neg: Float, neu: Float,
                            pos: Float, compound: Float, bin: SentimentType) {
  def toLabeledPoint: LabeledPoint = {
    // LabeledPoint(this.compound, Vectors.dense(this.body.split(" ").map(_.trim)))
    LabeledPoint(this.compound, Vectors.dense(Array(0.0, 1.0)))
  }
}

object App {
  def cleanString(someStr: String): String = {
    someStr.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase
    // should also remove stop words
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WSBSentiment").setMaster("local")
    val sc = new SparkContext(conf)

    val comments = sc.textFile(args(0)).filter(_.length > consts.minCommentLen).map{line =>
      // Regex to split csv with quotes
      val splitLine = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      splitLine.length match {
        case 8 => {
          // Handling dataset malformatting
          if (splitLine(1)(0) == '{') {
            val body = splitLine(0).trim
            val neg = splitLine(3).trim.toFloat
            val neu = splitLine(4).trim.toFloat
            val pos = splitLine(5).trim.toFloat
            val compound = splitLine(6).trim.toFloat
            val bin = SentimentType.Neutral.findBinContinuous(splitLine(7).trim.toFloat)
            SentimentComment(cleanString(body), neg, neu, pos, compound, bin)
          } else {
            null
          }
        }
        case _ => null
      }
    }.filter(_ != null)

    // Create vocabulary
    val allWords = comments.flatMap(_.body.split(" ")).filter(_.length <= consts.maxWordLen)
    val wordsMap = allWords.collect.toSet.zipWithIndex.map{case (word, i) => (word, i)}.toMap

    // Convert body to list of word indices
    val sets = comments.map{comm =>
      val words = comm.body.split(" ").filter{word =>
        word.length <= consts.maxWordLen && wordsMap.contains(word)
      }.map{word =>
        wordsMap.get(word) match {
          case Some(x) => x.toDouble
          case _ => -1 // should not be reachable in train set. null was causing the type to be Any and I don't like that
        }
      }.filter(_ != -1)
      (comm.compound, words)
    }.map{case (sentiment, words) =>
      LabeledPoint(sentiment, Vectors.dense(words.map(_.toDouble)))
    }.randomSplit(Array(0.6, 0.4), seed=42)

    val trainSet = sets(0)
    val testSet = sets(1)

    trainSet.take(10).foreach(println)

    // Train the model
    val nbModel = NaiveBayes.train(trainSet, lambda=1.0, modelType="multinomial")
  }
}
