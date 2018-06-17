package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * Created by itversity on 17/06/18.
  */
object GetDailyRevenueSQL {
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
    ordersDF.createTempView("orders")
    val orderItemsDF = spark.
      read.
      json(inputBaseDir + "/order_items")
    orderItemsDF.createTempView("order_items")

    val dailyRevenueDF = spark.
      sql("select order_date, sum(order_item_subtotal) order_item_subtotal " +
        "from orders join order_items " +
        "on order_id = order_item_order_id " +
        "where order_status in ('COMPLETE', 'CLOSED') " +
        "group by order_date")
    val outputBaseDir = envProps.getString("output.basedir")
    dailyRevenueDF.write.json(outputBaseDir + "/livedemodailyrevenuejsonsql")
  }

}
