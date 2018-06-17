package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * Created by itversity on 17/06/18.
  */
object GetDailyRevenueDF {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val executionMode = envProps.getString("execution.mode")
    val spark = SparkSession.
      builder.
      appName("Get Daily Revenue").
      master(executionMode).
      getOrCreate

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    val inputBaseDir = envProps.getString("input.json.basedir")
    val ordersDF = spark.
      read.
      json(inputBaseDir + "/orders")
    val orderItemsDF = spark.
      read.
      json(inputBaseDir + "/order_items")
    val ordersFilteredDF = ordersDF.
      where("order_status in ('COMPLETE', 'CLOSED')")
    val ordersJoinDF = ordersFilteredDF.
      join(orderItemsDF, $"order_id" === $"order_item_order_id")
    val dailyRevenueDF = ordersJoinDF.
      groupBy("order_date").
      sum("order_item_subtotal").
      orderBy("order_date")
    val outputBaseDir = envProps.getString("output.basedir")
    dailyRevenueDF.write.json(outputBaseDir + "/livedemodailyrevenuejson")
  }

}
