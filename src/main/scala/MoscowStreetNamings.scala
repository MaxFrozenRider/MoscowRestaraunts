
object MoscowStreetNamings extends App with SparkSessionWrapper {

  def getNamings: List[String] = {
    spark
      .read
      .option("header", true)
      .option("delimiter", ";")
      .csv("src/main/resources/moscow_namings.csv")
      .select("UM_TYPEN")
      .rdd.map(row => row(0).toString)
      .collect().toList
  }

}
