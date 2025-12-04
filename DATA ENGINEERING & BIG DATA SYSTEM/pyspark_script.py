from pyspark.sql import SparkSession
import time
import os

FILENAME = "penjualan_mobil_toyota.csv"

# =================================================
# 1. INISIASI SPARK (Aman untuk Windows + Python 3.13)
# =================================================
spark = SparkSession.builder \
    .appName("SalesFrequency") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("========== SPARK ==========\n")

# =================================================
# 2. LOAD CSV MENGGUNAKAN DATAFRAME (PALING AMAN)
# =================================================
df = spark.read.csv(FILENAME, header=True, inferSchema=True)

print("DETAIL DATASET")
print(f"Nama File   : {FILENAME}")
print(f"Total Baris : {df.count():,}")
print(f"Kolom       : {df.columns}")
print("-" * 80)

# =================================================
# 3. KONVERSI DATAFRAME → RDD
# =================================================
rdd = df.rdd

# =================================================
# 4. MAP & REDUCEBYKEY (AMAN UNTUK WINDOWS)
# =================================================
print("01. Transformations: Map(Product → 1) & ReduceByKey...")

start = time.time()

mapped_rdd = rdd.map(lambda row: (row['product'], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

print("02. Action: collect()...")
results = reduced_rdd.collect()

end = time.time()

# =================================================
# 5. SORT DAN TAMPILKAN OUTPUT
# =================================================
results.sort(key=lambda x: x[1], reverse=True)

print("\nHASIL FREKUENSI PRODUK (TOP 20)")
print("-" * 60)
print(f"{'PRODUCT':<30} | {'FREQ':>10}")
print("-" * 60)

for product, count in results[:20]:
    print(f"{str(product):<30} | {count:>10,}")

print("-" * 60)
print(f"Waktu Eksekusi Spark: {end - start:.4f} detik")

spark.stop()
