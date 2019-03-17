package advanced.movies

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// Printing movie ids with highest number of reviews
object PopularMovies {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PopularMovie")
    val lines = sc.textFile("src/main/resources/ml-100k/u.data")
    // u.data is tab delimited
    // It is in the format (userId movieId rating timeStamp)
    // We split each line and only extract the movieId and convert it to (movieId,1) format
    val movies = lines.map(line=>(line.split("\t")(1).toInt,1))
    val moviesWithRatingsCount = movies.reduceByKey((acc,current)=>acc+current).map(tuple=>(tuple._2,tuple._1))
      .sortByKey(false).collect()
    for(movie <- moviesWithRatingsCount){
      println(s"${movie._2}:${movie._1}")
    }



  }
}
