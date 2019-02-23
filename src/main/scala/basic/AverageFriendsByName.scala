package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// Find out average number of friends by first name
object AverageFriendsByName {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val name = fields(1)
    val numFriends = fields(3).toInt
    (name, numFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","AverageFriendByName")
    val lines = sc.textFile("src/main/resources/fakefriends.csv")
    // Parse the lines to get the (name,totalFriends) pair
    val records = lines.map(parseLine)
    // Convert the (name,totalFriends) to (name,(totalFriends,1))
    val mappedRecords = records.mapValues(totalFriends=>(totalFriends,1))
    val countedRecords = mappedRecords.reduceByKey((acc,current)=>(acc._1+current._1,acc._2+current._2))
    val averageFriendsByName = countedRecords.mapValues(friendRecord=>friendRecord._1/friendRecord._2)
    averageFriendsByName.sortByKey().foreach(println)

  }
}
