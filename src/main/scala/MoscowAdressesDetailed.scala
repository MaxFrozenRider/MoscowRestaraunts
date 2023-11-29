import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MoscowAdressesDetailed extends App with SparkSessionWrapper {
  val df = spark.read.option("header", true).option("delimiter", ";").csv("src/main/resources/moscow_addresses.csv")

  def doTransform(df: DataFrame): DataFrame = {
    df
      .filter("global_id <> 'global_id'")
      .select(
        col("P7").as("street"),
        col("P5").as("mini_district"),
        col("ADM_AREA").as("district")
      )
      .distinct()
  }

  doTransform(df).write
    .mode("overwrite")
    .parquet("src/main/resources/data/MoscowAddresses")


}
