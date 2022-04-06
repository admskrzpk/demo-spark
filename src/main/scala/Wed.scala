import org.apache.spark.sql.functions._
object Wed extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    //.config("spark.some.config.option", "some-value")
    .master("local[*]")
    .getOrCreate()

  val sample100 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("Sample100.csv")

   val newFile =  sample100
     .withColumn("Employee Markme", upper(sample100("Employee Markme")))
     .show
}



//  val write = df.write.text("plik4.txt")

// Morning Exercise
// DataFrame (Dataset)
// 1. Load CSV file
// 2. Dataset.withColumn + functions object
//  - https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html
//  - Values should all be UPPER case
// 3. DataFrame.show