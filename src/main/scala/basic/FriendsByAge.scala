package basic

import org.apache.log4j._
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")
    // The records in the file read, line by line
    // Sample line: 0,Will,33,385(index 2 is age, index 3 is totalFriends for Will)
    val records = sc.textFile("src/main/resources/fakefriends.csv")
    // Extract only age and totalFriends
    val rddRecords = records.map(parseLine)
    // Convert the records from (age,totalFriends) to (age,(totalFriends,1) format
    val rddRecordsWithCounter = rddRecords.mapValues(totalFriends => (totalFriends, 1))
    // Add totalFriends as well as the counter
    val rddRecordsWithStats = rddRecordsWithCounter.reduceByKey((acc, current) =>
      (acc._1 + current._1, acc._2 + current._2)
    )
    // Now the rdd will be in the format of (age,(totalFriendsThatTheAgeHas, totalPeopleInTheAge))
    // This format has total number of friends that a certain age has and total number of people in that age
    // Find out the average number of friends for that age
    // This will provide an age and the average number of friends that the age has
    val rddWithFriendsByAge = rddRecordsWithStats.mapValues(ageRecord => (ageRecord._1 / ageRecord._2))
    rddWithFriendsByAge.sortByKey().foreach(println)
  }

}
