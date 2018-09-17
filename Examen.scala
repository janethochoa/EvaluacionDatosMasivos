////1
import org.apache.spark.sql.SparkSession

val exa = SparkSession.builder().getOrCreate()
////2
val ja= spark.read.option("header", "true").option("inferSchema","true")csv("Netflix_2011_2016.csv")
ja.show()
////3
ja.columns
////4
ja.printSchema()
////5
ja.select($"Date",$"Open",$"High",$"Low",$"Close").show()
////6
ja.describe().show()
////7
val ja2=ja.withColumn("HV Ratio",ja("High")+ja("Close"))
////8
ja.orderBy($"High".desc).show(1)
////9
ja.select(mean("Close")).show()
////10
ja.select(max("Volume")).show()
ja.select(min("Volume")).show()
////11
  ///a
  ja.filter($"Close"<600).count()
  ///b
  (ja.filter($"High" > 500).count() * 1.0/ ja.count())*100
  ///c
  ja.select(corr("High","Volume")).show()
  ///d
  val yja = ja.withColumn("Year",year(ja("Date")))
  val ymax = yja.select($"Year",$"High").groupBy("Year").max()
  val res = ymax.select($"Year",$"max(High)")
  res.orderBy("Year").show()
  ///e
  val mja = ja.withColumn("Month",month(ja("Date")))
  val mavgs = mja.select($"Month",$"Close").groupBy("Month").mean()
  mavgs.select($"Month",$"avg(Close)").orderBy("Month").show()
