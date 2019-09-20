import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

object Practice extends App {
  val spark: SparkSession = SparkSession.builder.master("local[*]").appName("Part2 Practice").getOrCreate()
  import spark.implicits._

  val userEvents: DataFrame = spark.read.format("json").load("omni_clickstream.json").cache
  val products: DataFrame = spark.read.format("json").load("products.json").cache
  val users: DataFrame = spark.read.format("json").load("users.json").cache

  val window: WindowSpec = Window.partitionBy($"GENDER_CD").orderBy($"count".desc)

  //userEvents.withColumn("new_swid", regexp_replace(regexp_replace($"swid", "\\}", ""), "\\{", ""))
  userEvents
    .join(products, Seq("url"))
    .join(users, userEvents.col("swid") === concat(lit("{"), users("SWID"), lit("}")))
    .filter($"GENDER_CD" =!= "U")
    .groupBy($"GENDER_CD", $"category")
    .count
    .withColumn("rn", row_number().over(window))
    .filter($"rn" < 4)
    .show

}
