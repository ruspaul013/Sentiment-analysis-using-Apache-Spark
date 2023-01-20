from pyspark.ml.classification import NaiveBayesModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

def tokenize_idf(inputDF):
    tokenizer = Tokenizer(inputCol="word", outputCol="words")
    wordsData = tokenizer.transform(inputDF)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
    featurizedData = hashingTF.transform(wordsData)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    return idfModel.transform(featurizedData)

def getAnalysisTextBlob(text):
    score = TextBlob(text).sentiment.polarity
    if score < 0:
        return "Negative"
    elif score == 0:
        return "Neutral"
    else:
        return "Positive"

def getAnalysisModel(score):
    if score == 0:
        return "Negative"
    elif score == 1:
        return "Neutral"
    else:
        return "Positive"


def text_classification(words,model):
    # polarity detection
    polarity_result_udf = udf(getAnalysisTextBlob, StringType())
    words = words.withColumn("textblob",polarity_result_udf("word"))

    # words = words.withColumn("naivebayes",)
    return words

if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.cores",2) \
        .getOrCreate()
    nb_model=NaiveBayesModel.load("naive_bayes")
  
    # read the tweet data from socket
    lines = spark.readStream.format("socket").option("host", "0.0.0.0").option("port", 5555).load()
    # Preprocess the data
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words,nb_model)

    words = words.repartition(1)
    query = words.writeStream.queryName("all_tweets")\
        .outputMode("append").format("console").trigger(processingTime='60 seconds').start()
    query.awaitTermination()