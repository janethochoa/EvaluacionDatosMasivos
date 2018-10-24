//librerias que se ocupan para la limpieza de los datos e iniciar sesion
import org.apache.spark.sql.SparkSession
import spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
 // inicio de sesion para lectura de los datos
val spark = SparkSession.builder.master("local[*]").getOrCreate()
 val df = spark.read.option("inferSchema","true").csv("Iris.csv").toDF(
  "SepalLength", "SepalWidth", "PetalLength", "PetalWidth","class"
)
//Empieza la limpieza de los datos
val newcol = when($"class".contains("Iris-setosa"), 1.0).
  otherwise(when($"class".contains("Iris-virginica"), 3.0).
  otherwise(2.0))
 val newdf = df.withColumn("etiqueta", newcol)
newdf.select("etiqueta","SepalLength", "SepalWidth", "PetalLength", "PetalWidth","class").show(150, false)
 //todas las columnas en una sola, es decir, juntar los datos en uno mismo
val assembler = new VectorAssembler()  .setInputCols(Array("SepalLength", "SepalWidth", "PetalLength", "PetalWidth","etiqueta")).setOutputCol("features")
//Transformar datos
val features = assembler.transform(newdf)
features.show(5)
 // Anexar los labels añadiendo un metadato a la columna label.
// que estan en el dataset para incluir los labels en el index
val labelIndexer = new StringIndexer().setInputCol("class").setOutputCol("indexedLabel").fit(features)
println(s"Found labels: ${labelIndexer.labels.mkString("[", ", ", "]")}")
 // Automaticamente  identifica categoricamente los features, y los indexa.
// añade  maxCategories para que las features cont > 4 distintos valores  sean tratadoscomo continuo.
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(features)
 //Variables de entrenamiento , test y los porcentajes al azar
val splits = features.randomSplit(Array(0.6, 0.4))
val trainingData = splits(0)
val testData = splits(1)
 // la arquitecturas de las capas para la red neuronal:
// la capa de entrada con tamaño 4 (features), dos intermediarios tamaño 5 y 4
//  y de salida tañamo 3 (por las clases)
val layers = Array[Int](4, 2, 3, 3)
 // creamos el entrenador y se ponen los parametros
val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setBlockSize(128).setSeed(System.currentTimeMillis).setMaxIter(200)
 // Convierte los labels indexados devuelta a los labels originales
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
 // Encadena los indexados y la  MultilayerPerceptronClassifier en una  Pipeline.
// Se usa para que se procese el flujo de trabajo, aprende la prediccion del modelo
// Usando los features de los vectores o labels
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))
 // Entrena el modelo tmabien corre los indexados.
val model = pipeline.fit(trainingData)
 // Para hacer las predicciones
val predictions = model.transform(testData)
predictions.show(5)
 // Seleciona prediccion, original label
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy)) //Hace el test de error
