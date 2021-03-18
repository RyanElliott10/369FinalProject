import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection._
import co.theasi.plotly._
import co.theasi.plotly.{Plot, draw, writer}

import scala.math.BigDecimal.double2bigDecimal
import scala.util.Random


object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // imports / setup
    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val cwd = new java.io.File(".").getCanonicalPath
    val stockTSPATH = cwd + "/src/main/python/stockData/"

    val stockTS = sc.textFile(stockTSPATH + "output.csv")
    val header = stockTS.first()
    val data = stockTS.filter(row=>row!=header).
      map(x=> {
        val temp = x.split(",")
        (temp(1), (temp(2).toDouble, temp(3))) // (date, (price, close))
      }).
      filter(x=> x._2._2 != "open")

    val start = data.take(1)(0)._2._1

    // add in % change for point labels
    val dataNew = data.map(x=>{
      val diff = 10.0*(x._2._1/start - 1).setScale(4, BigDecimal.RoundingMode.HALF_UP)
      (x._1 + " : " + diff.toString() + "%", x._2 )
    })


    val ytest = data.map(x=>x._2._1).collect
    val xtest = (1 to (ytest.length-1).toInt).toList
    val pointLabels = dataNew.map(x=>x._1).collect

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
