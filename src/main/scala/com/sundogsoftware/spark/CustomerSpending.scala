package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object CustomerSpending {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CustomerSpending")

    val input = sc.textFile("data/customer-orders.csv")

    val overall_spending = input.map(x => x.split(','))

    val customer_spending = overall_spending.map(x => (x(0).toInt, x(2).toFloat))

    val customer_spending_total = customer_spending.reduceByKey( (x,y) => x + y )

    val customer_spending_total_sorted = customer_spending_total.sortBy(_._2)

    var results = customer_spending_total_sorted.collect()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- results) {
      val id = result._1
      val spending = result._2
      println(s"$id: $spending")
    }
  }

}
