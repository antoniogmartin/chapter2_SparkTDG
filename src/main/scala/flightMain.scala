import org.apache.spark
import org.apache.spark.mllib
import org.apache.spark.sql.SparkSession
import org.spark_project.dmg.pmml.False
import org.apache.spark.sql.functions.desc //this for queries in dataframe way

object flightMain {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder
      .master("local[*]")
      .appName("Word count")
      .getOrCreate()

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/2015-summary.csv")
    //set partitions to 5
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    //we can see how it will execute sort across the cluster by looking explain
    //flightData2015.sort("count").explain()
    //take by default not printing...
    flightData2015.sort("count").take(2)
    //print take
    flightData2015.sort("count").take(10).foreach(println)
    //Mostramos el dataset.
    flightData2015.show(10)
    //dataframe to table or view in order to make queries
    flightData2015.createOrReplaceTempView("flight_data_2015")

    /*
     //Explaining sql and dataframe_way
    val sqlWay = spark.sql("""
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)
    sqlWay.explain()

     val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()

    dataFrameWay.explain()

    spark.sql("SELECT max(count) from flight_data_2015").show()
    */
    val maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY sum(count) DESC
    LIMIT 5
    """)
    maxSql.show()
    println("hHOLASRFDGJOSDLTÑKIJSDF")
    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

  }
}
