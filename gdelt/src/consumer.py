import pickle
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Verify resources and download them if they are not available
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', download_dir='/home/uservob/nltk_data')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', download_dir='/home/uservob/nltk_data')

# Create the Spark session
spark = SparkSession.builder \
    .appName("KafkaDataStream") \
    .getOrCreate()

# Define the schema of the data
schema = StructType([
    StructField("date", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("pre", StringType(), True),
    StructField("quote", StringType(), True),
    StructField("post", StringType(), True)
])

# Create DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.11.10:9094") \
    .option("subscribe", "gdelt-news") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Cast or convert the value column to a string data type and
# rename it to 'json_value'
df = df.selectExpr("CAST(value AS STRING) as json_value")

# This will parse the JSON and apply the schema
df = df.withColumn("data", from_json(col("json_value"), schema)).select("data.*")

# Keep all columns except pre and post
df = df.drop("pre", "post")

# Add new columns with initial value 0
df_with_emotions = df \
    .withColumn("anger", lit(0)) \
    .withColumn("love", lit(0)) \
    .withColumn("fear", lit(0)) \
    .withColumn("happiness", lit(0)) \
    .withColumn("sadness/pain", lit(0))

# Text preprocessing
stop_words_sp = set(stopwords.words('spanish'))
stop_words_en = set(stopwords.words('english'))

def preprocess_text_sp(text):
    tokens = word_tokenize(text.lower())  # Aquí se usa 'punkt' para tokenizar el texto
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words_sp]
    return " ".join(filtered_tokens)

def preprocess_text_en(text):
    tokens = word_tokenize(text.lower())  # Aquí se usa 'punkt' para tokenizar el texto
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words_en]
    return " ".join(filtered_tokens)

# Filtered rows by language
df_sp = df.filter(col("lang") == "spanish")
df_en = df.filter(col("lang") == "english")

preprocess_udf_sp = udf(preprocess_text_sp, StringType())
preprocess_udf_en = udf(preprocess_text_en, StringType())

# Load SVM
with open('/opt/kafka/gdelt/svm-model/spanish-emotions-model.pkl', 'rb') as model_file:
    multi_label_svm_sp, tfidf_vectorizer_sp = pickle.load(model_file)

with open('/opt/kafka/gdelt/svm-model/english-emotions-model.pkl', 'rb') as model_file:
    multi_label_svm_en, tfidf_vectorizer_en = pickle.load(model_file)

# Function SVM
def predict_emotions_sp(text):
    processed_text = preprocess_text_sp(text)
    text_tfidf = tfidf_vectorizer_sp.transform([processed_text])
    predictions = multi_label_svm_sp.predict(text_tfidf)
    return predictions[0].tolist()

def predict_emotions_en(text):
    processed_text = preprocess_text_en(text)
    text_tfidf = tfidf_vectorizer_en.transform([processed_text])
    predictions = multi_label_svm_en.predict(text_tfidf)
    return predictions[0].tolist()

predict_emotions_udf_sp = udf(predict_emotions_sp, ArrayType(IntegerType()))
predict_emotions_udf_en = udf(predict_emotions_en, ArrayType(IntegerType()))

# SVM on 'quote' and update emotions columns
df_final_sp = df_with_emotions.withColumn("emotions", predict_emotions_udf_sp(col("quote")))
df_final_en = df_with_emotions.withColumn("emotions", predict_emotions_udf_en(col("quote")))

# Merge dataframes
df_final = df_final_sp.union(df_final_en)

df_final = df_final.withColumn("anger", col("emotions")[0]) \
    .withColumn("love", col("emotions")[1]) \
    .withColumn("fear", col("emotions")[2]) \
    .withColumn("happiness", col("emotions")[3]) \
    .withColumn("sadness/pain", col("emotions")[4]) \
    .drop("emotions")

# Function to process each batch
def process_batch(batch_df, batch_id):
    # Aquí puedes incluir más procesamiento de data si es necesario
    print(f"Batch {batch_id}:\n")
    batch_df.show(3)
    # Guarda el batch en HDFS
    batch_df.write.format("parquet").mode("append").save("hdfs://bda-project:9000/kafka/data")

query = df_final.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://bda-project:9000/kafka/checkpoint") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()