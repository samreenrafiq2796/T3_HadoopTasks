
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("Penguins Analysis") \
    .getOrCreate()

try:
    # Read cleaned CSV from HDFS
    df = spark.read.csv(
        "hdfs:///user/maria_dev/T3Task/penguins_clean.csv",
        header=True,
        inferSchema=True
    )

    # Display dataset schema
    print("Penguins Dataset Schema:")
    df.printSchema()

    # Display first five records
    df.show(5)

    # Select relevant columns
    df = df.select(
        "species",
        "island",
        "flipper_length_mm",
        "body_mass_g",
        "body_mass_kg"
    )

    # Filter records with flipper length below or equal to zero
    df = df.filter(col("flipper_length_mm") <= 0)

    # Sort records by flipper length
    df = df.orderBy("flipper_length_mm")

    # Cache the results
    df.cache()

    # Display filtered records
    print("Filtered and Sorted Records:")
    df.show()

    # Display total number of records
    print("Total Records:", df.count())

    # Save output as JSON, partitioned by species
    output_path = "hdfs:///user/maria_dev/spark/output/penguins"

    df.write \
        .mode("overwrite") \
        .partitionBy("species") \
        .json(output_path)

    print("JSON output saved successfully to HDFS:")
    print(output_path)

    # Verify output files
    print("Output saved successfully.")

finally:
    # Stop Spark session properly
    spark.stop()
    print("Spark session stopped.")
