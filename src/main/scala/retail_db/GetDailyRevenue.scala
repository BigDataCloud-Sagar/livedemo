package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by itversity on 17/06/18.
  */
object GetDailyRevenue {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val executionMode = envProps.getString("execution.mode")
    val conf = new SparkConf().
      setMaster(executionMode).
      setAppName("Get Daily Revenue")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inputBaseDir = envProps.getString("input.basedir")
    val orders = sc.textFile(inputBaseDir + "/orders")
    val orderItems = sc.textFile(inputBaseDir + "/order_items")
    val ordersFiltered = orders.
      filter(o => List("COMPLETE", "CLOSED").contains(o.split(",")(3)))
    val ordersFilteredMap = ordersFiltered.
      map(o => (o.split(",")(0).toInt, o.split(",")(1)))
    val orderItemsMap = orderItems.
      map(o => (o.split(",")(1).toInt, o.split(",")(4).toFloat))
    val ordersJoin = ordersFilteredMap.
      join(orderItemsMap)
    val ordersJoinMap = ordersJoin.map(o => o._2)
    val dailyRevenue = ordersJoinMap.reduceByKey(_ + _)
    val dailyRevenueSorted = dailyRevenue.sortByKey()

    val outputBaseDir = envProps.getString("output.basedir")
    dailyRevenueSorted.
      map(o => o.productIterator.mkString("\t")).
      saveAsTextFile(outputBaseDir + "/livedemodailyrevenue")
  }

}
