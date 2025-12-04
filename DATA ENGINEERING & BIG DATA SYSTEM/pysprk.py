from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, sum as sum_

# Buat Spark Session
spark = SparkSession.builder \
    .appName("PenjualanToyota") \
    .getOrCreate()

# Load dataset
df = spark.read.csv("penjualan_mobil_toyota.csv", header=True, inferSchema=True)

# Convert kolom Bulan → type date
df = df.withColumn("Bulan", col("Bulan").cast("date"))

# Ambil tahun
df = df.withColumn("Tahun", year(col("Bulan")))

# Hitung total penjualan per tahun
hasil = df.groupBy("Tahun").agg(sum_("Penjualan").alias("Total_Penjualan"))

# Simpan output ke HDFS/output folder
hasil.write.csv("output_penjualan_toyota_tahunan", header=True)

# Tampilkan hasil
hasil.show()
