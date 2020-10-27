from pyspark import SparkContext, SparkConf
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.shell import spark


def something(context):
    dataset = spark.read.csv(header='true', inferSchema='true', path='../docker/airflow/data/data.csv')
    dataset.withColumn("Quantity", dataset["Quantity"].cast("double")) \
        .withColumn("UnitPrice", dataset["UnitPrice"].cast("float")) \
        .withColumn("CustomerID", dataset["CustomerID"].cast("double"))
    vector_assembler = VectorAssembler(inputCols=['UnitPrice', 'CustomerID'],
                                      outputCol='features')
    vectorized_dataset = vector_assembler.setHandleInvalid("skip").transform(dataset)
    vectorized_dataset = vectorized_dataset.select(['features', 'Quantity'])
    lr = LinearRegression(featuresCol='features', labelCol='Quantity', maxIter=10,
                          regParam=0.3, elasticNetParam=0.8)
    splits = vectorized_dataset.randomSplit([0.7, 0.3])
    train_df = splits[0]
    test_df = splits[1]
    lr_model = lr.fit(train_df)
    print("Coefficients: " + str(lr_model.coefficients))
    print("Intercept: " + str(lr_model.intercept))
    training_summary = lr_model.summary
    print("RMSE: %f" % training_summary.rootMeanSquaredError)
    print("r2: %f" % training_summary.r2)
    lr_predictions = lr_model.transform(test_df)
    lr_predictions.select("prediction", "Quantity", "features").show(5)
    lr_evaluator = RegressionEvaluator(predictionCol="prediction",
                                       labelCol="Quantity", metricName="r2")
    print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(
        lr_predictions))
    lr_model.save('/tmp/regressor.model')


if __name__ == '__main__':
    conf = SparkConf().setAppName("app").setMaster("spark://localhost:7077")
    sc = SparkContext.getOrCreate(conf=conf)
    something(sc)
