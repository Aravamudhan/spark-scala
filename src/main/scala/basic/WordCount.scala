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
    // Because of this all the lines are combined together and split word by word
    val rawWords = lines.flatMap(word=>word.split(" "))

    // Replace any special character with empty string. In this I wanted to remove characters like comma and dot
    val words = rawWords.map(word=>word.replaceAll("\\W",""))

    // Convert a word to a (word,1) tuple format. For example if there is a word "hadoop" it will become
    // (hadoop,1). This helps in counting number of words. For example if there are 5 "king" words, then
    // all become (king,1) which is useful in the reduceByKey to count the total number of occurrences of
    // the word "king"
    val wordMap = words.map(word=>(word,1))

    // Now this reduceByKey method simply groups all the tuples by the keys and adds all their values
    // Say there are 5 tuples with the pair (king,1), then it becomes (king,5).
//    val wordsWithCount = wordMap.reduceByKey((key1, key2)=>{
//       key1+key2
//    })

    // The function (key1,key2)=>({key1+key2}) can be written as (_+_)
    val wordsWithCount = wordMap.reduceByKey(_+_)

    wordsWithCount.sortBy(record=>record._2, false).foreach(println)


  }

}
