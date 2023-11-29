import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark = SparkSession.builder()
    .appName("SparkTest")
    .config("spark.master", "local")
    .getOrCreate()
}
