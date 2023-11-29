import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark = SparkSession.builder()
    .appName("SparkProd")
    .config("spark.master", "local")
    .getOrCreate()

}
