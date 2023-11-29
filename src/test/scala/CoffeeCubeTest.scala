import CoffeeCube._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class CoffeeCubeTest extends AnyFlatSpec  with SparkSessionTestWrapper
with BeforeAndAfter with DataFrameComparer {

  import spark.implicits._

  var testDf: DataFrame = _
  before {
    testDf = spark.read.option("header", true).csv("src/test/resources/coffee_cube/moscow_places_clean.csv")
  }

  it should "print correct schema getLanguagesRating and check correct number of strings" in {
    val expectedDf = Seq(10L).toDF("count(1)")

    doTransform(testDf).printSchema()

    val resultDf = doTransform(testDf)
      .agg(count("*"))

    assertSmallDatasetEquality(expectedDf, resultDf, orderedComparison = false)
  }
}
