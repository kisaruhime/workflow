import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    //getting path to file - "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/expedia/train.csv"
    val pathToFile = "src\\main\\resources\\data.csv"
    //creating sparkSession object with params for csv file and downloading it
    val spark = SparkSession.builder
      .master("local")
      .appName("workflow_task")
      .getOrCreate()

    //creating dataframe with params
    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(pathToFile)

    //creating temp table
    df.createOrReplaceTempView("train")

    //sql query to dataframe
    val subpr1 = spark.sql("SELECT CONCAT(hotel_continent,\"-\", hotel_country,\"-\",hotel_market) as hotel, COUNT(*) " +
      "as count FROM train WHERE srch_adults_cnt = 2" +
      " GROUP BY CONCAT(hotel_continent,\"-\", hotel_country,\"-\",hotel_market) ORDER BY count DESC LIMIT 3")
    //showing results
    subpr1.show()
    //stopping session
    spark.stop()

  }

}