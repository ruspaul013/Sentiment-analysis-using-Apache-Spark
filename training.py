from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import StringIndexer

def preprocessing(lines):
    words = lines
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
    words = words.withColumn('text', F.regexp_replace('text', '@\w+', ''))
    words = words.withColumn('text', F.regexp_replace('text', '#', ''))
    words = words.withColumn('text', F.regexp_replace('text', 'RT', ''))
    words = words.withColumn('text', F.regexp_replace('text', ':', ''))
    return words

def tokenize_idf(inputDF):
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(inputDF)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
    featurizedData = hashingTF.transform(wordsData)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    return idfModel.transform(featurizedData)

spark = SparkSession.builder.appName("Train NaiveBayes") \
    .config("spark.driver.memory", "6g") \
    .config("spark.driver.cores",2) \
    .getOrCreate()

inputDF=spark.read.options(delimiter=',').csv("training_data.csv",inferSchema=True,header=True,quote='"',multiLine=True)
inputDF=preprocessing(inputDF)
inputDF=tokenize_idf(inputDF)
inputDF.repartition(100)

l_indexer  = StringIndexer(inputCol="category", outputCol="labelIndex")
inputDF = l_indexer.fit(inputDF).transform(inputDF)
(trainingData, testData) = inputDF.randomSplit([0.6, 0.4])

nb = NaiveBayes(labelCol="labelIndex",featuresCol="features", smoothing=1.0,modelType="multinomial")
model = nb.fit(trainingData)
predictions = model.transform(testData)
evaluator =MulticlassClassificationEvaluator(labelCol="labelIndex",predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
print("Accuracy = %g" %(accuracy))
print(model)

model.write().overwrite().save("naive_bayes")