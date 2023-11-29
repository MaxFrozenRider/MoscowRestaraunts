import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object MoscowPlacesClean extends App with SparkSessionWrapper {
  import spark.implicits._

  val df = spark.read.option("header", true).csv("src/main/resources/moscow_places.csv")

  private val moscowAddresses = spark.read.parquet("src/main/resources/data/MoscowAddresses")

  private val streetsNamings = MoscowStreetNamings.getNamings

  private val getStreet = (address : String) => {
    val arr = address.split(", ")

    var street = ""

    for (x <- arr) {
      for (naming <- streetsNamings) {
        if (x.contains(naming)) street = x
      }
    }
    street
  }

  private val getStreetUDF = udf(getStreet)

  def doTransform(df: DataFrame): DataFrame = {
    val windowForDuplicates = Window
      .partitionBy(
        "name",
        "category",
        "address",
        "district",
        "hours",
        "lat",
        "lng",
        "rating",
        "price",
        "avg_bill",
        "middle_avg_bill",
        "middle_coffee_cup",
        "chain",
        "seats"
      ).orderBy("name")

    df
      .filter("name != 'Кафе'")
      .withColumn("name", lower(col("name")))
      .withColumn("row_number", row_number.over(windowForDuplicates))
      .filter("row_number == 1") //делаем дедупликацию
      .drop("row_number")
      .withColumn("street", getStreetUDF($"address"))
      .join(moscowAddresses, Seq("street", "district"), "left")
      .withColumn("is_24_7", col("hours").like("%круглосуточно%") &&
        col("hours").like("%ежедневно%"))
      .groupBy(
        "name",
        "category",
        "address",
        "district",
        "hours",
        "lat",
        "lng",
        "rating",
        "price",
        "avg_bill",
        "middle_avg_bill",
        "middle_coffee_cup",
        "chain",
        "seats",
        "street",
        "is_24_7"
      )
      .agg(
        collect_list("mini_district").as("mini_districts") // Дедубликация из-за длинных проспектов и улиц
      )
      .withColumn("mini_districts", concat_ws(", ", col("mini_districts")))
  }

  doTransform(df).write.mode("overwrite").parquet("src/main/resources/data/MoscowPlacesClean")

}
