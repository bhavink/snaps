# Databricks notebook source
# MAGIC %md # IDP Use Case
# MAGIC **Business case:**  
# MAGIC Insurance fraud is a huge problem in the industry. It's difficult to identify fraud claims. IHS is in a unique position to help the Auto Insurance industry with this problem.  
# MAGIC 
# MAGIC **Problem Statement:**  
# MAGIC Data is stored in different systems and its difficult to build analytics using multiple data sources. Copying data into a single platform is time consuming.  
# MAGIC 
# MAGIC **Business solution:**  
# MAGIC Use S3 as a data lake to store different sources of data in a single platform. This allows data scientists / analysis to quickly analyze the data and generate reports to predict market trends and/or make financial decisions.  
# MAGIC 
# MAGIC **Technical Solution:**  
# MAGIC Use Databricks as a single platform to pull various sources of data from API endpoints, or batch dumps into S3 for further processing. ETL the CSV datasets into efficient Parquet formats for performant processing.  

# COMMAND ----------

# MAGIC %md
# MAGIC In this example, we will be working with some auto insurance data to demonstrate how we can create a predictive model that predicts if an insurance claim is fraudulent or not. This will be a Binary Classification task, and we will be creating a Decision Tree model.
# MAGIC 
# MAGIC With the prediction data, we are able to estimate what our total predicted fradulent claim amount is like, and zoom into various features such as a breakdown of predicted fraud count by insured hobbies - our model's best predictor.
# MAGIC       
# MAGIC We will cover the following steps to illustrate how we build a Machine Learning Pipeline:
# MAGIC * Data Import
# MAGIC * Data Exploration
# MAGIC * Data Processing
# MAGIC * Create Decision Tree Model
# MAGIC * Measuring Error Rate
# MAGIC * Model Tuning
# MAGIC * Zooming in on Prediction Data

# COMMAND ----------

# MAGIC %md ## Data Import
# MAGIC 
# MAGIC The data used in this example was from a CSV file that was imported using the Tables UI.
# MAGIC 
# MAGIC After uploading the data using the UI, we can run SparkSQL queries against the table, or create a DataFrame from the table.  
# MAGIC In this example, we will create a Spark DataFrame.

# COMMAND ----------

# MAGIC %fs cp -r /mnt/motsol/IDP.csv /tmp/motsol/IDP.csv

# COMMAND ----------

# MAGIC %fs ls /mnt/motsol

# COMMAND ----------



# COMMAND ----------

df = spark.read\
          .options(inferSchema="true", header="true")\
          .csv("/mnt/motsol/IDP.csv")

# COMMAND ----------

df.write.parquet("/tmp/idp/parquet/")

# COMMAND ----------



# COMMAND ----------

#save data frame as parquet - optimized data storage format
df.write.parquet("/tmp/idp/parquet/")
#create a new dataframe using parquet
parquetIDPDF = spark.read.parquet("/tmp/idp/parquet/")
#cache dataframe
parquetIDPDF.cache()
#display data
display(parquetIDPDF)
#run a query and store results in a dataframe
sqldf = spark.sql("select * from parquet.`/tmp/idp/parquet/` limit 15")
#save query result for future
sqldf.write.parquet("/tmp/idp/query/allrecords")

# COMMAND ----------

df.printSchema

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as NumRows from parquet.`/tmp/idp/parquet/` where MetaEventSource rlike("eventsource://detroitmi.gov")

# COMMAND ----------

display(sqldf)

# COMMAND ----------

df.createOrReplaceTempView("bk_idp")

# COMMAND ----------

df.cache()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import com.esri.core.geometry._
# MAGIC 
# MAGIC def isWithinBounds(point: Seq[Double], bounds: Seq[Seq[Double]]): Boolean = {
# MAGIC   val _point = new Point(point(0), point(1))
# MAGIC   
# MAGIC   val _bounds = new Polygon()
# MAGIC   _bounds.startPath(bounds(0)(0), bounds(0)(1))
# MAGIC   _bounds.lineTo(bounds(1)(0), bounds(1)(1))
# MAGIC   _bounds.lineTo(bounds(2)(0), bounds(2)(1))
# MAGIC   _bounds.lineTo(bounds(3)(0), bounds(3)(1))
# MAGIC   
# MAGIC   OperatorWithin.local().execute(_point, _bounds, SpatialReference.create("WGS84"), null)
# MAGIC }
# MAGIC 
# MAGIC val isWithin = udf(isWithinBounds(_: Seq[Double], _: Seq[Seq[Double]]))
# MAGIC 
# MAGIC import org.apache.spark.sql.expressions._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val boundingBox1 = ("ORD", Array(Array(-88.265139, 42.155232), Array(-88.265139, 41.467784), Array(-87.516527, 42.155232), Array(-87.516527, 41.467784)))
# MAGIC 
# MAGIC //val boundingBox1 = ("NYC", Array(Array(-74.431, 40.293), Array(-74.431, 41.286), Array(-72.276, 40.293), Array(-72.276, 41.286)))
# MAGIC val boundingBox2 = ("Baltimore/DC", Array(Array(-76.686, 38.335), Array(-76.686, 39.47), Array(-76.0, 38.335), Array(-76.0, 39.47)))
# MAGIC 
# MAGIC val bounds = sc.parallelize(Seq(boundingBox1, boundingBox2)).toDF("name", "bounds")
# MAGIC 
# MAGIC val boundingBox = bounds.filter($"name" === "ORD").select($"bounds").collect().head.getAs[Seq[Seq[Double]]](0)
# MAGIC 
# MAGIC val withVoyage2 = spark.sql(s""" select longitude, latitude, count(*) as weight from bk_idp_csv group by longitude, latitude limit 100""")
# MAGIC val points = withVoyage2.select($"longitude".as("lng"), $"latitude".as("lat")).toJSON.collect().mkString(", ")
# MAGIC val weight = withVoyage2.select($"weight".as("weight")).toJSON.collect().mkString(", ")
# MAGIC 
# MAGIC val ne = s"{lng: ${boundingBox(3)(0)}, lat: ${boundingBox(3)(1)}}"
# MAGIC val sw = s"{lng: ${boundingBox(0)(0)}, lat: ${boundingBox(0)(1)}}"
# MAGIC 
# MAGIC 
# MAGIC displayHTML(s"""<!DOCTYPE html>
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <meta name="viewport" content="initial-scale=1.0">
# MAGIC     <meta charset="utf-8">
# MAGIC     <style>
# MAGIC        body {
# MAGIC          margin: 4px;
# MAGIC        }
# MAGIC        #map {
# MAGIC         width: 1100px;
# MAGIC         height: 500px;
# MAGIC       }
# MAGIC     </style>
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <div id="map"></div>
# MAGIC     <script>
# MAGIC       function initMap() {
# MAGIC         var map = new google.maps.Map(document.getElementById('map'), {
# MAGIC           zoom: 8
# MAGIC         });
# MAGIC         
# MAGIC         map.fitBounds(new google.maps.LatLngBounds($sw, $ne))
# MAGIC         
# MAGIC         var infowindow = new google.maps.InfoWindow();
# MAGIC         
# MAGIC         map.addListener('click', function() {
# MAGIC           infowindow.close()
# MAGIC         });
# MAGIC         
# MAGIC         map.data.setStyle(function(feature) {
# MAGIC           var color = 'gray';
# MAGIC           return ({
# MAGIC             icon: null,
# MAGIC             fillColor: color,
# MAGIC             strokeColor: color,
# MAGIC             strokeWeight: 2
# MAGIC           });
# MAGIC         });        
# MAGIC         
# MAGIC         map.data.addListener('click', function(event) {
# MAGIC           infowindow.close();
# MAGIC           var myHTML = 'foo';
# MAGIC           infowindow.setContent("<div style='width:150px; text-align: center;'>"+myHTML+"</div>");
# MAGIC           infowindow.setPosition(event.feature.getGeometry().get());
# MAGIC           infowindow.setOptions({pixelOffset: new google.maps.Size(0,-30)});
# MAGIC           infowindow.open(map);          
# MAGIC         }); 
# MAGIC         
# MAGIC         var heatmap = new google.maps.visualization.HeatmapLayer({
# MAGIC           data: [$points].map(function(i) { return {location: new google.maps.LatLng(i),weight: 1
# MAGIC           }; })
# MAGIC         });
# MAGIC         
# MAGIC         heatmap.setMap(map);
# MAGIC         heatmap.set('opacity', 1.0);
# MAGIC       }
# MAGIC     </script>
# MAGIC     <script async defer
# MAGIC     src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBziwAG-zzHVG8a0-Q6fUA5gopVBQemJxo&callback=initMap&libraries=visualization">
# MAGIC     </script>
# MAGIC   </body>
# MAGIC </html>""")

# COMMAND ----------

# MAGIC %sql desc bk_idp_csv

# COMMAND ----------

# MAGIC %sql select latitude, longitude from bk_idp_csv where latitude != 0 and longitude != 0

# COMMAND ----------

# Preview data
display(df)

# COMMAND ----------

data = spark.read\
          .options(inferSchema="true", header="true")\
          .csv("/mnt/motsol/datasets/IDP.csv")\
          .drop("_c39")

df = data.withColumn("policy_bind_date", data.policy_bind_date.cast("string"))\
         .withColumn("incident_date", data.incident_date.cast("string"))

# COMMAND ----------

# MAGIC %md ## Data Exploration
# MAGIC 
# MAGIC We have several string (categorical) columns in our dataset, along with some ints and doubles.

# COMMAND ----------

display(df.dtypes)

# COMMAND ----------

# MAGIC %md Count number of categories for every categorical column (Count Distinct).

# COMMAND ----------

# Create a List of Column Names with data type = string
stringColList = [i[0] for i in df.dtypes if i[1] == 'string']
print stringColList

# COMMAND ----------

from pyspark.sql.functions import *

# Create a function that performs a countDistinct(colName)
distinctList = []
def countDistinctCats(colName):
  count = df.agg(countDistinct(colName)).collect()
  distinctList.append(count)

# COMMAND ----------

# Apply function on every column in stringColList
map(countDistinctCats, stringColList)
print distinctList

# COMMAND ----------

# MAGIC %md 
# MAGIC We have identified that some string columns have many distinct values (900+). We will remove these columns from our dataset in the Data Processing step to improve model accuracy.
# MAGIC * policy number (1000 distinct)
# MAGIC * policy bind date (951 distinct. Possible to narrow down to year/month to test model accuracy)
# MAGIC * insured zip (995 distinct)
# MAGIC * insured location (1000 distinct)
# MAGIC * incident date (60 distinct. Excluding, but possible to narrow down to year/month to test model accuracy)

# COMMAND ----------

# MAGIC %md Like most fraud datasets, our label distribution is skewed.

# COMMAND ----------

display(df)

# COMMAND ----------

# Count number of frauds vs non-frauds
display(df.groupBy("fraud_reported").count())

# COMMAND ----------

# MAGIC %md We can quickly create one-click plots using Databricks built-in visualizations to understand our data better.
# MAGIC 
# MAGIC Click 'Plot Options' to try out different chart types.

# COMMAND ----------

# Fraud Count by Incident State
display(df)

# COMMAND ----------

# Breakdown of Average Vehicle claim by insured's education level, grouped by fraud reported
display(df)

# COMMAND ----------

# MAGIC %md ## Data Processing
# MAGIC 
# MAGIC Next, we will clean up the data a little and prepare it for our machine learning model.
# MAGIC 
# MAGIC We will first remove the columns that we have identified earlier that have too many distinct categories and cannot be converted to numeric.

# COMMAND ----------

colsToDelete = ["policy_number", "policy_bind_date", "insured_zip", "incident_location", "incident_date"]
filteredStringColList = [i for i in stringColList if i not in colsToDelete]

# COMMAND ----------

# MAGIC %md %md We will convert categorical columns to numeric to pass them into various algorithms. This can be done using the StringIndexer.
# MAGIC 
# MAGIC Here, we are generating a StringIndexer for each categorical column and appending it as a stage of our ML Pipeline.

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

transformedCols = [categoricalCol + "Index" for categoricalCol in filteredStringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index") for categoricalCol in filteredStringColList]
stages

# COMMAND ----------

# MAGIC %md As an example, this is what the transformed dataset will look like after applying the StringIndexer on all categorical columns.

# COMMAND ----------

from pyspark.ml import Pipeline

indexer = Pipeline(stages=stages)
indexed = indexer.fit(df).transform(df)
display(indexed)

# COMMAND ----------

# MAGIC %md Use the VectorAssembler to combine all the feature columns into a single vector column. This will include both the numeric columns and the indexed categorical columns.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# In this dataset, numericColList will contain columns of type Int and Double
numericColList = [i[0] for i in df.dtypes if i[1] != 'string']
assemblerInputs = map(lambda c: c + "Index", filteredStringColList) + numericColList

# Remove label from list of features
label = "fraud_reportedIndex"
assemblerInputs.remove(label)
assemblerInputs
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# Append assembler to stages, which currently contains the StringIndexer transformers
stages += [assembler]

# COMMAND ----------

# MAGIC %md Generate transformed dataset. This will be the dataset that we will use to create our machine learning models.

# COMMAND ----------

pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(df)
transformed_df = pipelineModel.transform(df)

# Rename label column
transformed_df = transformed_df.withColumnRenamed('fraud_reportedIndex', 'label')

# Keep relevant columns (original columns, features, labels)
originalCols = df.columns
selectedcols = ["label", "fraud_reported", "features"] + originalCols
dataset = transformed_df.select(selectedcols)
display(dataset)

# COMMAND ----------

# MAGIC %md By selecting "label" and "fraud reported", we can infer that 0 corresponds to **No Fraud Reported** and 1 corresponds to **Fraud Reported**.

# COMMAND ----------

# MAGIC %md Next, split data into training and test sets.

# COMMAND ----------

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
print trainingData.count()
print testData.count()

# COMMAND ----------

# MAGIC %md Databricks makes it easy to use multiple languages in the same notebook for your analyses. Just register your dataset as a temporary table and you can access it using a different language!

# COMMAND ----------

# Register data as temp table to jump to Scala
trainingData.createOrReplaceTempView("trainingData")
testData.createOrReplaceTempView("testData")

# COMMAND ----------

# MAGIC %md ## Create Decision Tree Model
# MAGIC 
# MAGIC We will create a decision tree model in Scala using the trainingData. This will be our initial model.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel}
# MAGIC 
# MAGIC // Create DataFrames using our earlier registered temporary tables
# MAGIC val trainingData = spark.table("trainingData")
# MAGIC val testData = spark.table("testData")
# MAGIC 
# MAGIC // Create initial Decision Tree Model
# MAGIC val dt = new DecisionTreeClassifier()
# MAGIC   .setLabelCol("label")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setMaxDepth(5)
# MAGIC   .setMaxBins(40)
# MAGIC 
# MAGIC // Train model with Training Data
# MAGIC val dtModel = dt.fit(trainingData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Make predictions on test data using the transform() method.
# MAGIC // .transform() will only use the 'features' column as input.
# MAGIC val dtPredictions = dtModel.transform(testData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // View model's predictions and probabilities of each prediction class
# MAGIC val selected = dtPredictions.select("label", "prediction", "probability")
# MAGIC display(selected)

# COMMAND ----------

# MAGIC %md ## Measuring Error Rate
# MAGIC 
# MAGIC Evaluate our initial model using the BinaryClassificationEvaluator.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
# MAGIC 
# MAGIC // Evaluate model
# MAGIC 
# MAGIC val evaluator = new BinaryClassificationEvaluator()
# MAGIC evaluator.evaluate(dtPredictions)

# COMMAND ----------

# MAGIC %md ## Model Tuning
# MAGIC 
# MAGIC We can tune our models using built-in libraries like `ParamGridBuilder` for Grid Search, and `CrossValidator` for Cross Validation. In this example, we will test out a combination of Grid Search with 5-fold Cross Validation.  
# MAGIC 
# MAGIC Here, we will see if we can improve accuracy rates from our initial model.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // View tunable parameters for Decision Trees
# MAGIC dt.explainParams

# COMMAND ----------

# MAGIC %md Create a ParamGrid to perform Grid Search. We will be adding various values of maxDepth and maxBins.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
# MAGIC 
# MAGIC val paramGrid = new ParamGridBuilder()
# MAGIC   .addGrid(dt.maxDepth, Array(3, 10, 15))
# MAGIC   .addGrid(dt.maxBins, Array(40, 50))
# MAGIC   .build()

# COMMAND ----------

# MAGIC %md Perform 5-fold Cross Validation.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Create 5-fold CrossValidator
# MAGIC val cv = new CrossValidator()
# MAGIC   .setEstimator(dt)
# MAGIC   .setEvaluator(evaluator)
# MAGIC   .setEstimatorParamMaps(paramGrid)
# MAGIC   .setNumFolds(5)
# MAGIC 
# MAGIC // Run cross validations
# MAGIC val cvModel = cv.fit(trainingData)

# COMMAND ----------

# MAGIC %md We can print out what our Tree Model looks like using toDebugString.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC val bestTreeModel = cvModel.bestModel.asInstanceOf[DecisionTreeClassificationModel]
# MAGIC println("Learned classification tree model:\n" + bestTreeModel.toDebugString)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Use test set here so we can measure the accuracy of our model on new data
# MAGIC val cvPredictions = cvModel.transform(testData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // cvModel uses the best model found from the Cross Validation
# MAGIC // Evaluate best model
# MAGIC evaluator.evaluate(cvPredictions)

# COMMAND ----------

# MAGIC %md Using the same evaluator as before, we can see that Cross Validation improved our model's accuracy from 0.732 to 0.841!

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(bestTreeModel)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC println(bestTreeModel.featureImportances)

# COMMAND ----------

# MAGIC %md We know that feature 9 is our Decision Tree model's root node and that 5 is a close 2nd in importance. Let's see what they correspond to.

# COMMAND ----------

print assemblerInputs[9]
print assemblerInputs[5]

# COMMAND ----------

# MAGIC %md Turns out that incident severity and insured hobbies are the best predictors for whether an insurance claim is fraudulent or not!

# COMMAND ----------

# MAGIC %md ## Zooming in on Prediction Data
# MAGIC 
# MAGIC We can further analyze the resulting prediction data. As an example, we can view an estimate of what our total predicted fradulent claim amount is like, and zoom into a breakdown of predicted fraud count by incident severity and insured hobbies since those our model's best predictors.
# MAGIC 
# MAGIC Lets hop back to Python for this.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC cvPredictions.createOrReplaceTempView("cvPredictions")

# COMMAND ----------

# Select columns to zoom into (In this example: Total Claim Amount and Auto Make)
# Filter for data points that were predicted to be Fraud cases
cvPredictions = sqlContext.sql("SELECT * FROM cvPredictions")
incidentSeverityDF = cvPredictions.select("prediction", "total_claim_amount", "incident_severity").filter("prediction = 1")
insuredHobbiesDF = cvPredictions.select("prediction", "total_claim_amount", "insured_hobbies").filter("prediction = 1")

# COMMAND ----------

# View Count of Predicted Fraudulent Claims by Incident Severity
display(incidentSeverityDF)

# COMMAND ----------

# View Count of Predicted Fraudulent Claims by Insured Hobbies
display(insuredHobbiesDF)

# COMMAND ----------

# MAGIC %md Looks like people who are in major accidents and play chess or are into cross-fit are more prone to committing fraud.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ##APPENDIX
# MAGIC ###Gradient Boosting Trees and Random Forrest
# MAGIC ###Modeling and Evaluation

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.classification.{GBTClassifier, GBTClassificationModel}
# MAGIC 
# MAGIC // Create initial Decision Tree Model
# MAGIC val gbt = new GBTClassifier()
# MAGIC   .setLabelCol("label")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setMaxDepth(5)
# MAGIC   .setMaxBins(40)
# MAGIC 
# MAGIC // Train model with Training Data
# MAGIC val gbtModel = gbt.fit(trainingData)
# MAGIC 
# MAGIC // Make predictions on test data using the transform() method.
# MAGIC // .transform() will only use the 'features' column as input.
# MAGIC val gbtPredictions = gbtModel.transform(testData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
# MAGIC 
# MAGIC // Create initial Decision Tree Model
# MAGIC val rf = new RandomForestClassifier()
# MAGIC   .setLabelCol("label")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setMaxDepth(5)
# MAGIC   .setMaxBins(40)
# MAGIC 
# MAGIC // Train model with Training Data
# MAGIC val rfModel = rf.fit(trainingData)
# MAGIC 
# MAGIC // Make predictions on test data using the transform() method.
# MAGIC // .transform() will only use the 'features' column as input.
# MAGIC val rfPredictions = rfModel.transform(testData)

# COMMAND ----------

# MAGIC %scala
# MAGIC println("GBT Eval - " + evaluator.evaluate(gbtPredictions))
# MAGIC println("RF Eval - " + evaluator.evaluate(rfPredictions))

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC rf.explainParams

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
# MAGIC 
# MAGIC // We use a ParamGridBuilder to construct a grid of parameters to search over.
# MAGIC // TrainValidationSplit will try all combinations of values and determine best model using
# MAGIC // the evaluator.
# MAGIC val paramGrid = new ParamGridBuilder()
# MAGIC   .addGrid(rf.maxBins, Array(40, 50))
# MAGIC   .addGrid(rf.numTrees, Array(20, 30, 40))
# MAGIC   .addGrid(rf.maxDepth, Array(3, 10, 15))
# MAGIC   .build()
# MAGIC 
# MAGIC // // In this case the estimator is simply the linear regression.
# MAGIC // // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
# MAGIC // val trainValidationSplit = new TrainValidationSplit()
# MAGIC //   .setEstimator(rf)
# MAGIC //   .setEvaluator(evaluator)
# MAGIC //   .setEstimatorParamMaps(paramGrid)
# MAGIC //   // 80% of the data will be used for training and the remaining 20% for validation.
# MAGIC //   .setTrainRatio(0.6)
# MAGIC 
# MAGIC // // Run train validation split, and choose the best set of parameters.
# MAGIC // val model = trainValidationSplit.fit(trainingData)
# MAGIC 
# MAGIC // Create 5-fold CrossValidator
# MAGIC val cv = new CrossValidator()
# MAGIC   .setEstimator(dt)
# MAGIC   .setEvaluator(evaluator)
# MAGIC   .setEstimatorParamMaps(paramGrid)
# MAGIC   .setNumFolds(5)
# MAGIC 
# MAGIC // Run cross validations
# MAGIC val model = cv.fit(trainingData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val tvsPredictions = model.transform(testData)
# MAGIC evaluator.evaluate(tvsPredictions)

# COMMAND ----------

