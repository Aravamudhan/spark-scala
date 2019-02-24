package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// Read a file, split it into words, remove special characters, group by the words, count each word
object WordCount {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")

    // Reads the file line by line
    val lines = sc.textFile("src/main/resources/words.txt")

    // It flattens all the entries of the lines RDD and applies the given function on it
    // If there are 5 lines in a file, then the map function produces 5 lines. map function
    // has one-to-one mapping with the RDD. But the flatMap can have one-to-many relationship with the
    // input and the output rows or one-to-one relationship too. Using the split function through flatMap
    // on the lines RDD will product multiple lines where each line is single word. One line of "lines" RDD
    // is split into multiple lines with each line containing single word
    val words = lines.flatMap(word => word.split("\\W+"))

    // Convert the words to smaller case to normalize
    val lowerCasedWords = words.map(word => word.toLowerCase())

    // Convert a word to a (word,1) tuple format. For example if there is a word "hadoop" it will become
    // (hadoop,1). This helps in counting number of words. For example if there are 5 "king" words, then
    // all become (king,1) which is useful in the reduceByKey to count the total number of occurrences of
    // the word "king"
    val wordMap = lowerCasedWords.map(word => (word, 1))

    // Now this reduceByKey method simply groups all the tuples by the keys and adds all their values
    // Say there are 5 tuples with the pair (king,1), then it becomes (king,5).
    //    val wordsWithCount = wordMap.reduceByKey((accumulatedValue, current)=>{
    //       accumulatedValue + currentValue
    //    })

    // The function (accumuated,current)=>({accumuated+current}) can be written as (_+_)
    // Flip the key and value to change it from (word,wordCount) to (wordCount,word so that it can be sorted by key
    val wordsWithCountSorted = wordMap.reduceByKey(_ + _).map(tuple => (tuple._2, tuple._1)).sortByKey(false)
    for (record <- wordsWithCountSorted) {
      println(s"${record._2}:${record._1}")
    }

  }

}
