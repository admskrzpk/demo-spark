import org.apache.spark.sql.functions._

object Wed extends App {

  import org.apache.spark.sql.SparkSession

  val path = if (args(0).nonEmpty) sample100.csv(args(0))
  else sample100.csv("Sample100.csv")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  val sample100 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")

  val newFile = path
    .withColumn("Employee Markme", upper(path("Employee Markme")))
    .show
}