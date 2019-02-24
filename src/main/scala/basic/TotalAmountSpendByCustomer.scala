package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

// Find out total amount spent by a customer
object TotalAmountSpendByCustomer {

  def parseLines(line:String)={
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amountSpent = fields(2).toFloat
    (customerId,amountSpent)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","AmountSpentByCustomer")
    val lines = sc.textFile("src/main/resources/customer-orders.csv")
    val records = lines.map(parseLines)
    val totalAmountSpent = records.reduceByKey((accumulated, current)=>(accumulated+current))
      .map(tuple=>(tuple._2,tuple._1)).sortByKey(false)
    val results = totalAmountSpent.collect()
    println("CusomterId:TotalAmountSpent")
    println("---------------------------")
    for(result <- results){
      val customerId = result._2
      val amountSpent = result._1
      println(s"${customerId}:${amountSpent}")
    }


  }
}
