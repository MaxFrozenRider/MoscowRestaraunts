import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

import java.util.Properties

object CoffeeCube extends App with SparkSessionWrapper {
  val df = spark.read.parquet("src/main/resources/data/MoscowPlacesClean")

  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.setProperty("user", "postgres")
  connectionProperties.setProperty("password", "123456Qw")

  def doTransform(df: DataFrame): DataFrame = {
    df
      .filter("category == 'кофейня'")
      .filter("seats < 400")
  }

  doTransform(df)
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .jdbc(
      url = "jdbc:postgresql://localhost:5432/restaraunts",
      table = "public.coffee_cube",
      connectionProperties = connectionProperties
    )

}
