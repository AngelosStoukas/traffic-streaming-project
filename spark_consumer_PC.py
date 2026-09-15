from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Μπορειτε να χρησιμοποιειτε παρενθέσεις ( ... ) γύρω από μπλοκ κώδικα για να επιτρέψετε αλλαγή γραμμής (multi-line statement) χωρίς να χρειάζεται να χρησιμοποιείτε τον χαρακτήρα backslash \


# 1. Ορισμός του Schema
## vehicles_to_pandas returns a pd.DataFrame:
# A DataFrame containing the travel logs of vehicles, with the columns:
#
# 'name': the name of the vehicle (platoon).
# 'dn': the platoon size.
# 'orig': the origin node of the vehicle's trip.
# 'dest': the destination node of the vehicle's trip.
# 't': the timestep.
# 'link': the link the vehicle is on (or relevant status).
# 'x': the position of the vehicle on the link.
# 's': the spacing of the vehicle.
# 'v': the speed of the vehicle.
schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("t", DoubleType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())


# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 3. Σύνδεση στον Redpanda (Kafka-compatible)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "uxsim")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parsing του JSON και Μετασχηματισμός
# Χρησιμοποιείται "cast" για να μετατραπούν οι raw binary τιμες σε ευαναγνωστο JSON string
# Εφαρμοστε το schema στο string, δημιουργώντας ενα μοναδικό struct με την ονομασία data
# Καθε key το JSON (name, orig, dest, etc.) μετατρεπεται σε ξεχωριστη στήλη στο DataFrame.
parsed = (
    df.selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), schema).alias("data"))
      .select("data.*")
)

# 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
# t = time από την εξομοίωση, v = ταχύτητα οχήματος
# Στον παρακάτω κώδικα χρησιμοποιούμε το πεδίο t της UXsim ως "time". Αν η εξομοίωση στέλνει δεδομένα πολύ γρήγορα, το Spark θα τα ομαδοποιεί σωστά ανά simulation step.
stats = (
    parsed.groupBy(col("t").alias("time"), col("link"))
          .agg(
              count("*").alias("vcount"),
              avg("v").alias("vspeed")
          )
)


# 6α. Αποθήκευση στη MongoDB των αρχικών δεδομενων
query_raw = (
    parsed.writeStream
    .format("mongodb")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/raw")
    .option("spark.mongodb.connection.uri", "mongodb://mongo:27017")
    .option("database", "traffic")
    .option("collection", "raw_data")
    .start()
)

# 6β. Αποθήκευση στη MongoDB των επεξεργασμενων δεδομενων
# Το outputMode("append") στο streaming απαιτεί τη χρήση Watermarks (χρονικά όρια) για να ξέρει το Spark πότε "έκλεισε" ένα group και μπορεί να το γράψει.
# Διαφορετικά, χρησιμοποιούμε το .outputMode("update"), το οποίο γράφει στη MongoDB μόνο τα links που άλλαξαν οι τιμές τους.
# ΣΗΜΕΙΩΣΗ: Ο MongoDB Spark Connector δεν υποστηρίζει native streaming sink με outputMode("update")
# (πετάει IllegalArgumentException: "MongoTable() does not support Update mode").
# Λύση: χρησιμοποιούμε foreachBatch, το οποίο μετατρέπει κάθε micro-batch σε ένα απλό
# batch write (mode "append"), παρακάμπτοντας τον περιορισμό, ενώ διατηρούμε
# outputMode("update") στο ίδιο το streaming query ώστε να παίρνουμε μόνο τα links που άλλαξαν.
def write_stats_to_mongo(batch_df, batch_id):
    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option("spark.mongodb.connection.uri", "mongodb://mongo:27017")
        .option("database", "traffic")
        .option("collection", "stats")
        .save()
    )

query_mongo = (
    stats.writeStream
    .outputMode("update")
    .foreachBatch(write_stats_to_mongo)
    .option("checkpointLocation", "/tmp/checkpoints/stats")
    .start()
)

# 7. Προβολή στην κονσόλα για debugging (προαιρετικά)
query_console = (
    stats.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .start()
)


# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination() # query_mongo.awaitTermination()