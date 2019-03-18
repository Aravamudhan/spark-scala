package advanced.movies

import org.apache.spark.SparkContext
import org.apache.log4j._

/**
  * The superhero with most number of appearances
  * Marvel-graph.txt -> It is just numbers. The number is the id of a superhero
  * and all the subsequent numbers are the ids of the superheros that appeared
  * with.
  * Marvel-names.txt -> id to name mapping
  */
object MostPopularSuperhero {

//  Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    // Split the given string based on whitespaces
    // There could be tabs, tabs with spaces, it does not matter
    // It provides a list of strings. This is being used for splitting up
    // Marvel-graph.txt file
    val elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }

  // This function is used to discard invalid lines from the dataset
  def parseNames(line: String) : Option[(Int, String)] = {
    // The dataset is Marvel-names.txt which is in the format of
    // 1 "Some-name" (i.e.) NUMBER "SOME-TEXT". It is the format
    // in each line. So each line is split by double quotes
    // For the line [1 "Spiderman"] this split operation will
    // give ["1 ","Spiderman",""]
    val fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("src/main/resources/marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)

    // Load up the superhero co-apperarance data
    val lines = sc.textFile("src/main/resources/marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)

    val totalConnectionsBySuperheros = pairings.reduceByKey( (x,y) => x + y )

    // Flip it to # of connections, hero ID
    val flipped = totalConnectionsBySuperheros.map( x => (x._2, x._1) )

    // Find the max # of connections
    val mostPopular = flipped.max()

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }
}
