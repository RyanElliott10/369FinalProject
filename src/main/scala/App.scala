package WSBSentiment

import scala.io._
import scala.collection._
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.{ Logger, Level }

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
                            pos: Float, compound: Float, bin: SentimentType)

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WSBSentiment").setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).filter(_.length > 50).map(line => {
      // Regex to split with quotes and commas
      val splitLine = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      splitLine.length match {
        case 8 => {
          // Handling dataset malformatting issues
          if (splitLine(1)(0) == '{') {
            val body = splitLine(0).trim
            val neg = splitLine(3).trim.toFloat
            val neu = splitLine(4).trim.toFloat
            val pos = splitLine(5).trim.toFloat
            val compound = splitLine(6).trim.toFloat
            val bin = SentimentType.Neutral.findBinContinuous(splitLine(7).trim.toFloat)
            SentimentComment(body, neg, neu, pos, compound, bin)
          } else {
            null
          }
        }
        case _ => null
      }
    }).filter(_ != null).foreach(println)
  }
}
