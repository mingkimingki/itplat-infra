# Spesifikasi Tugas 2: Implementasi Pipeline Pemrosesan Batch dengan Hadoop MapReduce dan Spark

> **Draft Markdown** hasil konversi dari
> `Spesifikasi Tugas 2_ Pipeline Pemrosesan Batch dengan Hadoop MapReduce dan Spark.docx`.
> Isi mengikuti dokumen asli. Bagian bertanda *Catatan* adalah tambahan, bukan dari dosen.

## Ringkasan (dari README.txt)

- **Deskripsi tugas:** Mengimplementasikan dua versi pipeline pemrosesan batch untuk dataset yang sama
  (misal: log server atau data transaksi): (1) menggunakan Hadoop MapReduce, (2) menggunakan Spark
  (RDD/DataFrame). Bandingkan performa (waktu eksekusi, resource usage) dan menulis laporan analisis.
  Gunakan Google Colaboratory untuk memudahkan percobaannya.
- **Bentuk tugas:** Implementasi kode berupa Jupyter Notebook (`.ipynb`) + laporan komparasi performa.
- **Metode penilaian:** Kebenaran implementasi, kualitas kode, kedalaman analisis komparatif.
- **Waktu pengerjaan:** 10 hari.
- **Presentasi:** Membuat PPT di hari ke-11–14 untuk dipresentasikan di pertemuan 7 (**2 November 2026**).

---

Dokumen ini berisi spesifikasi teknis, panduan pengerjaan, skema pengujian performa, serta rubrik
penilaian untuk Tugas 2 Mata Kuliah Pemrosesan Data Terdistribusi / Big Data Analytics.

## 1. Deskripsi Umum & Latar Belakang

Dalam era Big Data, pemrosesan batch (*batch processing*) merupakan fondasi utama untuk menganalisis
data berukuran besar secara terdistribusi. Kerangka kerja Hadoop MapReduce memperkenalkan paradigma
komputasi berbasis disk (*disk-based storage and intermediate I/O*), sedangkan Apache Spark
menyempurnakannya dengan pendekatan *in-memory processing* yang dramatis meningkatkan kecepatan
eksekusi.

Pada tugas ini, mahasiswa diwajibkan mengimplementasikan dua versi pipeline pemrosesan batch untuk
dataset yang sama: (1) menggunakan paradigma Hadoop MapReduce, dan (2) menggunakan Apache Spark
(RDD dan DataFrame API). Selanjutnya, mahasiswa akan mengukur serta membandingkan performa kedua
kerangka kerja dari sudut pandang waktu eksekusi (*execution time*) dan konsumsi sumber daya
(*resource usage*/RAM & CPU) dalam lingkungan Google Colaboratory.

## 2. Tujuan Pembelajaran

1. Memahami arsitektur internal dan alur kerja komputasi MapReduce (Map, Shuffle, Reduce) serta Spark
   (Directed Acyclic Graph / DAG, Lazy Evaluation, In-Memory RDD/DataFrame).
2. Mampu membangun pipeline pemrosesan data terdistribusi berbasis Python menggunakan
   mrjob/Hadoop Streaming dan pyspark.
3. Mampu merancang metodologi benchmarking yang valid untuk mengukur performa komputasi Big Data.
4. Mampu menyusun laporan analitis kualitatif dan kuantitatif mengenai *trade-off* antara kecepatan,
   penggunaan memori, serta kompleksitas pengembangan kode.

## 3. Studi Kasus & Pilihan Dataset

Mahasiswa wajib memilih salah satu studi kasus berikut dengan dataset berukuran **minimal 100 MB
hingga 1 GB** agar karakteristik komputasi terdistribusi dapat teramati dengan jelas:

| Studi Kasus | Rekomendasi Dataset | Tujuan Pemrosesan Batch |
|---|---|---|
| **1. Log Web Server** | NASA HTTP Access Logs, Server Access Logs (Kaggle), atau Web Clickstream Logs. | - Agregasi jumlah request per IP Address / Endpoint.<br>- Distribusi HTTP Status Code (200, 404, 500).<br>- Total akumulasi traffic (bytes transferred) per jam. |
| **2. Transaksi e-Commerce / Finansial** | Online Retail II Dataset, Credit Card Transactions, atau E-commerce Sales Logs. | - Total nilai penjualan per kategori produk/negara.<br>- Frekuensi transaksi bulanan per pelanggan.<br>- Anomali transaksi berukuran ekstrem. |

## 4. Langkah-Langkah Implementasi di Google Colab

> *Catatan:* di docx, baris-baris kode menempel menjadi satu. Di bawah ini kode dipecah lagi per
> baris dan indentasinya dipulihkan, tanpa mengubah isinya.

### Tahap 1: Setup Lingkungan Kerja

Jalankan perintah berikut di sel awal Notebook Google Colab untuk menginstal Java JDK, PySpark, dan
mrjob:

```bash
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!pip install -q pyspark mrjob psutil
```

### Tahap 2: Implementasi MapReduce (Python mrjob)

Tulis skrip MapReduce menggunakan framework mrjob. Struktur kode harus mencakup fungsi Mapper dan
Reducer:

```python
%%writefile mapreduce_job.py
from mrjob.job import MRJob

class MRLogCounter(MRJob):
    def mapper(self, _, line):
        parts = line.split(" ")
        if len(parts) > 0:
            ip = parts[0]
            yield ip, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRLogCounter.run()
```

### Tahap 3: Implementasi Apache Spark (PySpark RDD & DataFrame)

Inisialisasi SparkSession dan buat pipeline menggunakan RDD API dan DataFrame API:

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("BatchProcessingBenchmarking") \
    .master("local[*]") \
    .getOrCreate()

# 1. Spark RDD API
rdd = spark.sparkContext.textFile("dataset.log")
rdd_counts = rdd.map(lambda line: (line.split(" ")[0], 1)).reduceByKey(lambda a, b: a + b)

# 2. Spark DataFrame API
df = spark.read.text("dataset.log")
from pyspark.sql.functions import split, col, count
df_counts = df.select(split(col("value"), " ").getItem(0).alias("ip")) \
              .groupBy("ip").agg(count("*").alias("total_request"))
```

### Tahap 4: Benchmarking & Measurement

Ukur durasi eksekusi dan konsumsi memori untuk tiap eksekusi menggunakan modul `psutil` dan `time`
Python:

```python
import psutil, os, time

def measure_execution(func):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)
    start_time = time.time()

    func()

    end_time = time.time()
    mem_after = process.memory_info().rss / (1024 * 1024)
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    print(f"Memory Usage Delta: {mem_after - mem_before:.2f} MB")
```

## 5. Format Luaran & Pengumpulan Tugas

Setiap kelompok/individu wajib mengumpulkan dua berkas utama:

1. **Jupyter Notebook File (`Tugas2_NIM_Nama.ipynb`):**
   - Harus dapat dijalankan ulang dari awal hingga akhir (*Restart & Run All*) tanpa error.
   - Memuat seluruh blok kode setup, MapReduce, Spark RDD, Spark DataFrame, dan visualisasi
     benchmarking.
2. **Laporan Analisis Komparatif (`Tugas2_NIM_Nama.pdf`, maksimal 5–7 halaman):**
   - **Bab 1: Pendahuluan:** Arsitektur data, penjelasan dataset, dan tujuan eksperimen.
   - **Bab 2: Hasil & Benchmarking:** Tabel komparasi eksekusi (waktu dalam detik, RAM dalam MB,
     CPU %).
   - **Bab 3: Analisis Komparatif:** Pembahasan mendalam mengenai faktor I/O Disk vs In-Memory
     Execution, biaya Shuffle, pembuatan DAG vs pembagian Map-Reduce.
   - **Bab 4: Kesimpulan:** Ringkasan rekomendasi pemilihan framework berdasarkan kasus nyata.

## 6. Rubrik Penilaian Detail

| Kriteria Penilaian | Bobot | Indikator Keberhasilan |
|---|---|---|
| **Kebenaran Implementasi Kode** | **35%** | - Implementasi MapReduce dan PySpark berjalan sempurna.<br>- Output dari ketiga pendekatan (MapReduce, Spark RDD, Spark DF) terbukti identik/valid. |
| **Kualitas Kode & Kerapian Notebook** | **20%** | - Kode bersih, terstruktur, mengikuti konvensi PEP8, serta dilengkapi komentar.<br>- Struktur notebook rapi dengan penjelas Markdown. |
| **Kedalaman Analisis Komparatif** | **35%** | - Menyajikan grafik/tabel benchmarking aktual.<br>- Analisis teknis mendalam (I/O overhead, caching, DAG execution, garbage collection). |
| **Ketepatan Waktu & Format** | **10%** | - Mengumpulkan berkas `.ipynb` dan `.pdf` tepat waktu sesuai tata cara pengumpulan. |
