package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import math.min
import math.max
import java.time.format.DateTimeFormatter
import java.time.LocalDate

/*
Find out minimum and maximum temperatures for a station in the given year 1800
 */
object TemperatureStats {

  def parseLines(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f
    (stationId, entryType, temperature)
  }

  def getMaxTemperature(lines: RDD[String]) = {
    // Get the records and filter only TMAX records. Records with TMAX type indicate maximum temperature
    val records = lines.map(parseLines).filter(record => record._2 == "TMAX")
    // TMAX (i.e.)entryType has served it's purpose in identifying the type of record. Now get rid of it
    // Convert the tuple from (stationId,entryType,temperature) to (stationId,temperature)
    val temperatureRecords = records.map(tuple => (tuple._1, tuple._3))
    // for every key get the minimum value of all the values
    val maximumTemperatures = temperatureRecords.reduceByKey((accumulated, current) => max(accumulated, current))
    val results = maximumTemperatures.collect()

    // Iterate through the list
    for (result <- results) {
      val station = result._1
      val temperature = result._2
      val formattedTemperature = f"$temperature%.2f C"
      println(s"$station's maximum temperature :$formattedTemperature")
    }
  }

  def getMaxPrecipitation(lines: RDD[String]) = {
    val records = lines.map(line => {
      val fields = line.split(",")
      val stationId = fields(0)
      val date = fields(1)
      val entryType = fields(2)
      val precipitation = fields(3).toFloat
      (stationId, date, entryType, precipitation)
    }).filter(record => record._3 == "PRCP")
    val precipitationRecords = records.map(tuple => (tuple._1, tuple._2, tuple._4))
    val maxPrecipitationRecord = precipitationRecords.reduce((tuple1, tuple2) => {
      if (tuple1._3 > tuple2._3)
        tuple1
      else
        tuple2
    })
    val stationId = maxPrecipitationRecord._1
    val date = maxPrecipitationRecord._2
    val precipitation = maxPrecipitationRecord._3
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val localDate = LocalDate.parse(date, dateFormatter)
    println(s"Day ${localDate.getDayOfMonth} of the month ${localDate.getMonth} on the year " +
      s"${localDate.getYear} in the station $stationId, highest precipitation of $precipitation was recorded")

  }

  def getMinimumTemperature(lines: RDD[String]) = {
    // Get the records and filter only TMIN records. Records with TMIN type indicate minimum temperature
    val records = lines.map(parseLines).filter(record => record._2 == "TMIN")
    // TMIN (i.e.)entryType has served it's purpose in identifying the type of record. Now get rid of it
    // Convert the tuple from (stationId,entryType,temperature) to (stationId,temperature)
    val temperatureRecords = records.map(tuple => (tuple._1, tuple._3))
    // for every key get the minimum value of all the values
    val minimumTemperatures = temperatureRecords.reduceByKey((accumulated, current) => min(accumulated, current))
    val results = minimumTemperatures.collect()

    // Iterate through the list
    for (result <- results) {
      val station = result._1
      val temperature = result._2
      val formattedTemperature = f"$temperature%.2f C"
      println(s"$station's minimum temperature :$formattedTemperature")
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TemperatureStats")
    val lines = sc.textFile("src/main/resources/1800.csv")
    getMinimumTemperature(lines)
    getMaxTemperature(lines)
    getMaxPrecipitation(lines)


  }
}
