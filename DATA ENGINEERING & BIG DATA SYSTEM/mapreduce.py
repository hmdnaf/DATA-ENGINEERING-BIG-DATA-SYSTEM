import csv
import time
import os
import math

FILENAME = "mini_sales_train_folds.csv"

# ======================
# 1. DESKRIPSI DATASET
# ======================
def print_dataset_info(path):
    size_mb = os.path.getsize(path) / (1024 * 1024)

    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        preview = [next(reader) for _ in range(3)]

    # Hitung total baris
    with open(path, 'rb') as f:
        total = sum(1 for _ in f) - 1

    print("DETAIL DATASET:")
    print(f"Nama File   : {os.path.basename(path)}")
    print(f"Ukuran      : {size_mb:.2f} MB")
    print(f"Total Baris : {total:,}")
    print(f"Kolom       : {header}")
    print("-" * 100)
    print("Preview 3 Baris:")
    for row in preview:
        print(row)
    print("-" * 100)


# =======================================
# 2. INPUT SPLITTING (Simulasi HDFS)
# =======================================
def load_input_as_blocks(filename, num_blocks=4):
    print(f"01. Input: Membaca '{os.path.basename(filename)}' dan memecah menjadi {num_blocks} blok...")
    
    data_blocks = []
    all_rows = []

    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            all_rows.append(row)

    total_rows = len(all_rows)
    chunk_size = math.ceil(total_rows / num_blocks)

    for i in range(num_blocks):
        start = i * chunk_size
        end = start + chunk_size
        block = all_rows[start:end]
        data_blocks.append(block)
        print(f"    -> Blok {i+1} berisi {len(block):,} baris.")

    return data_blocks


# ===========================
# 3. MAP TASK
# ===========================
def mapper(data_block):
    results = []
    for row in data_block:
        try:
            # Key yang dihitung: item_id (index 3)
            key = row[3]
            results.append((key, 1))
        except:
            continue
    return results


# ===========================
# 4. SHUFFLE & SORT
# ===========================
def shuffler(mapped_data_list):
    total_items = sum(len(x) for x in mapped_data_list)
    print(f"04. Shuffle & Sort: Mengelompokkan {total_items:,} pasangan (key,value)...")

    shuffled = {}
    for batch in mapped_data_list:
        for key, value in batch:
            if key not in shuffled:
                shuffled[key] = []
            shuffled[key].append(value)
    return shuffled


# ===========================
# 5. REDUCER
# ===========================
def reducer(shuffled_data):
    print(f"05. Reduce Task: Mengagregasi {len(shuffled_data):,} item unik...")
    reduced = {}
    for key, values in shuffled_data.items():
        reduced[key] = sum(values)
    return reduced


# ===========================
# 6. EKSEKUSI JOB
# ===========================
def submit_job(data_blocks):
    print("02. Job Submission: Mengirim blok ke Map Task...")
    print("03. Map Task: Memproses setiap blok menjadi (key, value)...")

    map_results = []
    for block in data_blocks:
        result = mapper(block)
        map_results.append(result)

    return map_results



print("\n========== SIMULASI MAPREDUCE ==========")
print_dataset_info(FILENAME)

start_cpu = time.process_time()

hdfs_blocks = load_input_as_blocks(FILENAME, num_blocks=4)
map_output = submit_job(hdfs_blocks)
shuffle_output = shuffler(map_output)
reduce_output = reducer(shuffle_output)

end_cpu = time.process_time()

# OUTPUT
print("\nHASIL AKHIR MAPREDUCE")
print("-" * 60)
print(f"{'ITEM ID':<20} | {'FREQ':>10}")
print("-" * 60)

sorted_final = sorted(reduce_output.items(), key=lambda x: x[1], reverse=True)

for item, count in sorted_final[:20]:  # hanya tampilkan 20 teratas
    print(f"{item:<20} | {count:>10,}")

print("-" * 60)
print(f"Waktu Komputasi MapReduce: {end_cpu - start_cpu:.4f} detik")
