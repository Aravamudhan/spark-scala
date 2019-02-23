package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MapByValueAndReduceByKey {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "LearnMapByValue&ReduceByKey")
    val array = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1),("a", 1), ("b", 1), ("b", 1),("b", 1), ("b", 1)), 3)
//     Keeps the keys the same but reassigns the value
//    val arrayMappedValue = array.mapValues(value => (value,1))
//    arrayMappedValue.foreach(println)

    val arrayByReducedKey=array.reduceByKey((key1,key2)=>{
      (key1+key2)
    })
    println("Reduced by key")
    arrayByReducedKey.foreach(println)
  }
}
