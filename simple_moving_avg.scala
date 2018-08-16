val stocks = spark.read.option("header","false").option("inferschemea","true").csv("/<<>>/week20180810/*")

//drop unwanted columns
val stocks_list = stocks.drop($"_c2").drop($"_c3").drop($"_c4").drop($"_c5").toDF("name","date","price")

//convert to date data type
val df = stocks_list.withColumn("date", to_date(col("date")))

//needed on the scala-ide
import org.apache.spark.sql.expressions.Window

//create the rolling window and find the average
val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
df.withColumn("moving_average",avg(df("price")).over(wSpec1)).show()


/*
+----+----+--------+------------------+                                         
|name|date|   price|    moving_average|
+----+----+--------+------------------+
| CLQ|null|  555457|          700892.5|
| CLQ|null|  846328|1924440.3333333333|
| CLQ|null| 4371536|2314714.6666666665|
| CLQ|null| 1726280| 8771274.333333334|
| CLQ|null|20216007|      1.09711435E7|
| CNU|null|  182820|          239043.0|
| CNU|null|  295266| 306716.6666666667|
| CNU|null|  442064| 296054.3333333333|
| CNU|null|  150833| 300008.6666666667|
| CNU|null|  307129|          228981.0|
| CSR|null| 1477494|         1822514.5|
| CSR|null| 2167535|1751973.3333333333|
| CSR|null| 1610891|1927965.3333333333|
| CSR|null| 2005470|         2379913.0|
| CSR|null| 3523378|         2764424.0|
| IHD|null|    5373|           13057.5|
| IHD|null|   20742|           10168.0|
| IHD|null|    4389|14324.333333333334|
| IHD|null|   17842|23943.333333333332|
| IHD|null|   49599|           33720.5|
+----+----+--------+------------------+
only showing top 20 rows
*/
