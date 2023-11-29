import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import java.util.Properties

object ChainsTop extends App with SparkSessionWrapper{
  val df = spark.read.parquet("src/main/resources/data/MoscowPlacesClean")

  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.setProperty("user", "postgres")
  connectionProperties.setProperty("password", "123456Qw")

  def doTransform(df: DataFrame): DataFrame = {
    df
      .filter("chain == 1")
      .filter("category == 'кофейня'")
      .groupBy("name")
      .agg(
        count("*").as("count_restaraunts")
      )
      .orderBy(desc("count_restaraunts"))
      .limit(15)
  }

  doTransform(df)
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .jdbc(
      url = "jdbc:postgresql://localhost:5432/restaraunts",
      table = "public.top_chains_report",
      connectionProperties = connectionProperties
    )

}
