import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import MoscowPlacesClean._

class MoscowPlacesCleanTest extends AnyFlatSpec  with SparkSessionTestWrapper
with BeforeAndAfter with DataFrameComparer {

  import spark.implicits._

  var testDf: DataFrame = _
  before {
    testDf = spark.read.option("header", true).csv("src/test/resources/moscow_places_clean/moscow_places_test.csv")
  }

  it should "print correct schema moscow_places_clean and check correct number of strings" in {
    val expectedDf = Seq(10L).toDF("count(1)")

    doTransform(testDf).printSchema()

    val resultDf = doTransform(testDf)
      .agg(count("*"))

    assertSmallDatasetEquality(expectedDf, resultDf, orderedComparison = false)
  }
}
