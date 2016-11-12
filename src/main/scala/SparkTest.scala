import org.apache.spark.sql.SparkSession

/**
  * Created by Dell on 07-Nov-2016.
  */
object SparkTest extends App{
    println("Hello Spark 2.0")

    val spark = SparkSession.builder().master(args(0))getOrCreate()
    val ds = spark.read.option("header",true).csv(args(1))
    ds.show()
    spark.stop()
    import spark.implicits._
    val ds1 = (1 to 10).toDF("id")
    ds1.show()
    spark.stop()
}

