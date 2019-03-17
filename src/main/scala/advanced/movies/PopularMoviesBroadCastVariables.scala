package advanced.movies

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

/**
  * This program finds out the most popular movies.
  * It is just like the PopularMovies.scala program except that by using the broadcast function
  * the program now have access to the movieNames as well. A broadcast variable can be used whenever
  * we want to cache a read only variable in each machine(where the program gets executed in
  * distributed manner)
  */
object PopularMoviesBroadCastVariables {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    // This map is used to store the movie names in the format (movieId,movieName)
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("src/main/resources/ml-100k/u.item").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PopularMovie")
    val nameDict = sc.broadcast(loadMovieNames)
    val lines = sc.textFile("src/main/resources/ml-100k/u.data")
    // u.data is tab delimited
    // It is in the format (userId movieId rating timeStamp)
    val movies = lines.map(line=>(line.split("\t")(1).toInt,1))
    // For each tuple of movieId, sum its value so that we can get the count for each movieId and
    // flip the tuple from (movieId,count) to (count,movieId). Then sort it by key, which is actually
    // sorting by the count
    val moviesWithRatingsCount = movies.reduceByKey((acc,current)=>acc+current).map(tuple=>(tuple._2,tuple._1))
      .sortByKey(false)
    // The movieWithRatingCount is converted to (movieName,count) from (count,movieId)
    val moviesWithNames = moviesWithRatingsCount.map(x=>(nameDict.value(x._2),x._1))
    val results = moviesWithNames.collect()
    results.foreach(println)



  }

}
