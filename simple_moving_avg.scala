val stocks = spark.read.option("header","false").option("inferschemea","true").csv("/<<>>/week20180810/*")

//drop unwanted columns
val stocks_list = stocks.drop($"_c2").drop($"_c3").drop($"_c4").drop($"_c5").toDF("name","date","price")

//convert to date data type
val df1 = stocks_list.withColumn("date", to_date(col("date")))

//needed on the scala-ide
import org.apache.spark.sql.expressions.Window

//create the rolling window and find the average
val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
df1.withColumn("moving_average",avg(df1("price")).over(wSpec1)).show()
