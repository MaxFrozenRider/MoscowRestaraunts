import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.util.Properties

object CategoriesReport extends App with SparkSessionWrapper {

  val df = spark.read.parquet("src/main/resources/data/MoscowPlacesClean")

  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.setProperty("user", "postgres")
  connectionProperties.setProperty("password", "123456Qw")

  def doTransform(df: DataFrame): DataFrame = {
    df
      .withColumn("total_restaraunts", count("*").over(Window.partitionBy()))
      .groupBy("category", "total_restaraunts")
      .agg(
        count("*").as("count_restaraunts"),
        avg("seats").as("avg_seats"),
        expr("percentile(seats, 0.5)").as("median_seats"),
        avg("rating").as("mean_rating")
      )
      .withColumn("ratio", col("count_restaraunts") / col("total_restaraunts"))
  }

  doTransform(df)
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .jdbc(
      url = "jdbc:postgresql://localhost:5432/restaraunts",
      table = "public.categories_report",
      connectionProperties = connectionProperties
    )

}
