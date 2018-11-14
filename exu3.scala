//1
import org.apache.spark.sql.SparkSession
//2
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
//3
val spark = SparkSession.builder().getOrCreate()
//4
import org.apache.spark.ml.clustering.KMeans
//5
val dataset  = spark.read.option("header","true").option("inferSchema", "true").csv("Wholesale customers data.csv")
dataset.printSchema
//6
val df = dataset.select($"Fresh",$"Milk",$"Grocery",$"Frozen",$"Detergents_Paper",$"Delicassen")
//7
import org.apache.spark.ml.feature.VectorAssembler

//8
val assembler = (new VectorAssembler()
                  .setInputCols(Array("Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen"))
                  .setOutputCol("features"))
//9
val features = assembler.transform(df)

//10
val kmeans = new KMeans().setK(3).setSeed(1L)
val model = kmeans.fit(features)

//11
val WSSSE = model.computeCost(features)
println(s"Within set sum of Squared Errors = $WSSSE")
//12
// Show results
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
