import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test

@Test
class DataTest {

  @Test
  def checkDataPositive(): Unit ={
    val sparkSession = SparkSession.builder().appName("task1").master("local").getOrCreate()
    //creating dataframe with params
    val df = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src\\test\\resources\\test_data_1.csv")

    //creating temp table
    df.createOrReplaceTempView("train")

    //sql query to dataframe
    val subpr1 = sparkSession.sql("SELECT CONCAT(hotel_continent,\"-\", hotel_country,\"-\",hotel_market) as hotel, COUNT(*) " +
      "as count FROM train WHERE srch_adults_cnt = 2" +
      " GROUP BY CONCAT(hotel_continent,\"-\", hotel_country,\"-\",hotel_market) ORDER BY count DESC LIMIT 3")

    //getting max value for checking
    val max = subpr1.first().get(1)

    //checking if result is correct
    assert(max == 3)
  }


}
