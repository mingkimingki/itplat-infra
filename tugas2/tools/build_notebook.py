"""Membuat notebook Tugas 2 (Kelompok 21) dari daftar sel di bawah.

Pemakaian: python build_notebook.py <path-output.ipynb>
"""
import sys

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook

CELLS = []


def md(text):
    CELLS.append(new_markdown_cell(text.strip("\n")))


def code(text):
    CELLS.append(new_code_cell(text.strip("\n")))


# ---------------------------------------------------------------- 0. Judul
md(r"""
# Tugas 2: Pipeline Pemrosesan Batch dengan Hadoop MapReduce dan Spark

**Mata kuliah:** Pemrosesan Data Terdistribusi / Big Data Analytics
**Kelompok 21**

| No | Nama | NIM |
|---|---|---|
| 1 | Eka Putra Kurnia | 25.55.2927 |
| 2 | Zahrul Amri Hutabarat | 25.55.2900 |
| 3 | Bujur Depoin Sianipar | 25.55.2879 |
| 4 | Riyan Pratomo Saputra | 25.55.2857 |

**Studi kasus:** Log Web Server — *NASA HTTP Access Logs* (Juli + Agustus 1995, ±356 MB, ±3,46 juta request).

**Tiga analisis batch** yang masing-masing dikerjakan dengan tiga pendekatan
(MapReduce/mrjob, Spark RDD, Spark DataFrame):

| Kode | Analisis | Key → value |
|---|---|---|
| `host` | Jumlah request per host (IP/nama host) | `host → 1`, dijumlah |
| `status` | Distribusi HTTP status code | `status → 1`, dijumlah |
| `hour` | Total traffic (bytes) per jam-dalam-sehari (00–23) | `jam → bytes`, dijumlah |

**Isi notebook:**
0. Pengaturan · 1. Setup lingkungan · 2. Persiapan dataset · 3. Parser bersama ·
4. MapReduce (mrjob) · 5. Spark RDD · 6. Spark DataFrame · 7. Validasi output ·
8. Alat ukur · 9. Benchmark · 10. Hasil benchmark · 11. Hasil analisis data · 12. Catatan untuk laporan
""")

# ---------------------------------------------------------------- 0. Pengaturan
md(r"""
## 0. Pengaturan

- `QUICK_MODE = True` hanya memakai `QUICK_LINES` baris pertama dataset. Gunakan untuk mengecek
  bahwa seluruh notebook berjalan tanpa error sebelum menjalankan benchmark penuh.
- `REPEATS` = berapa kali setiap pengukuran diulang (hasilnya dirata-rata).

Kedua nilai juga bisa diatur lewat *environment variable* dengan nama yang sama.
""")

code(r"""
import os

QUICK_MODE = os.environ.get("QUICK_MODE", "0") == "1"
QUICK_LINES = 200_000
REPEATS = int(os.environ.get("REPEATS", "3"))

DATA_DIR = "data"
RESULT_DIR = "hasil"
ANALYSES = ["host", "status", "hour"]

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)
print(f"QUICK_MODE={QUICK_MODE}, REPEATS={REPEATS}")
""")

# ---------------------------------------------------------------- 1. Setup
md(r"""
## 1. Setup Lingkungan Kerja

Mengikuti Tahap 1 spesifikasi, dengan tiga penyesuaian:

1. **Java 17, bukan Java 8.** `pip install pyspark` memasang PySpark 4.x, yang membutuhkan Java 17
   atau lebih baru. Dengan Java 8, Spark gagal dijalankan.
2. **`setuptools`** ikut dipasang karena mrjob 0.7.4 (rilis terakhir, 2020) masih memakai modul
   `distutils` yang sudah dihapus dari pustaka standar Python 3.12; `setuptools` menyediakan penggantinya.
3. **`standard-pipes`** dipasang bila Python ≥ 3.13, karena mrjob juga memakai modul `pipes`
   yang dihapus di Python 3.13.

Instalasi hanya dijalankan di Google Colab; di luar Colab sel ini memakai paket yang sudah terpasang.
""")

code(r"""
import importlib
import sys

IN_COLAB = "google.colab" in sys.modules

if IN_COLAB:
    !apt-get install -y openjdk-17-jdk-headless -qq > /dev/null
    !pip install -q pyspark mrjob psutil setuptools
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    if sys.version_info >= (3, 13):
        !pip install -q standard-pipes
    importlib.invalidate_caches()
else:
    print("Bukan di Google Colab: instalasi dilewati.")
""")

md(r"""
Spesifikasi perangkat lunak dan mesin (dicatat untuk Bab 1 laporan):
""")

code(r"""
import platform
import shutil
import subprocess

import mrjob
import psutil
import pyspark

java_bin = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if "JAVA_HOME" in os.environ else shutil.which("java")
java_version = subprocess.run([java_bin, "-version"], capture_output=True, text=True).stderr.splitlines()[0]

ENV_INFO = {
    "Sistem operasi": platform.platform(),
    "Python": platform.python_version(),
    "PySpark": pyspark.__version__,
    "mrjob": mrjob.__version__,
    "Java": java_version,
    "Inti CPU (logical)": psutil.cpu_count(logical=True),
    "RAM total (GB)": round(psutil.virtual_memory().total / 1024**3, 1),
}
for key, value in ENV_INFO.items():
    print(f"{key:<20}: {value}")
""")

# ---------------------------------------------------------------- 2. Dataset
md(r"""
## 2. Persiapan Dataset

Log diunduh dari *Internet Traffic Archive* (LBL), lalu kedua bulan digabung menjadi satu berkas teks.

**Normalisasi:** sebagian kecil baris memuat byte non-ASCII atau karakter kontrol (misalnya tab di
dalam URL). Regex Python (dipakai mrjob & RDD) dan regex Java (dipakai DataFrame) memperlakukan
karakter seperti itu sedikit berbeda, sehingga setiap byte di luar ASCII yang dapat dicetak diganti
dengan `?`. Dengan begitu ketiga pendekatan membaca teks yang persis sama.
""")

code(r"""
import gzip
import urllib.request
from pathlib import Path

LOG_URLS = [
    "https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz",
    "https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz",
]
FULL_PATH = Path(DATA_DIR) / "nasa_access_log.txt"
QUICK_PATH = Path(DATA_DIR) / "nasa_access_log_quick.txt"

# Tabel penerjemah: byte ASCII yang dapat dicetak (spasi s.d. ~) dipertahankan, sisanya jadi "?".
PRINTABLE_ONLY = bytes(b if 32 <= b <= 126 else ord("?") for b in range(256))

gz_paths = []
for url in LOG_URLS:
    gz_path = Path(DATA_DIR) / url.rsplit("/", 1)[-1]
    if not gz_path.exists():
        print(f"Mengunduh {url} ...")
        urllib.request.urlretrieve(url, gz_path)
    gz_paths.append(gz_path)

if not FULL_PATH.exists():
    with open(FULL_PATH, "wb") as out:
        for gz_path in gz_paths:
            with gzip.open(gz_path, "rb") as f:
                for raw in f:
                    out.write(raw.rstrip(b"\r\n").translate(PRINTABLE_ONLY) + b"\n")

if QUICK_MODE and not QUICK_PATH.exists():
    with open(FULL_PATH, "rb") as src, open(QUICK_PATH, "wb") as out:
        for i, raw in enumerate(src):
            if i >= QUICK_LINES:
                break
            out.write(raw)

DATA_PATH = str(QUICK_PATH if QUICK_MODE else FULL_PATH)
""")

code(r"""
with open(DATA_PATH, "rb") as f:
    total_lines = sum(1 for _ in f)

print(f"Berkas     : {DATA_PATH}")
print(f"Ukuran     : {os.path.getsize(DATA_PATH) / 1024**2:,.1f} MB")
print(f"Jumlah baris: {total_lines:,}")
print("\nContoh 3 baris pertama:")
with open(DATA_PATH) as f:
    for _ in range(3):
        print(" ", f.readline().rstrip())
""")

# ---------------------------------------------------------------- 3. Parser
md(r"""
## 3. Parser Bersama

Satu pola regex untuk mengambil **host, jam, status, dan bytes** dari setiap baris, disimpan di
`nasa_parser.py` agar dipakai oleh mrjob dan Spark RDD. Spark DataFrame memakai **pola yang sama**
lewat `regexp_extract`. Baris yang tidak cocok dengan pola dibuang dengan cara yang sama di ketiganya.

Format baris (*Common Log Format*):

```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
   host                 waktu (jam = 00)              request               status bytes
```

Catatan: kolom pertama tidak selalu IP; sering berupa nama host (misalnya `unicomp6.unicomp.net`).
""")

code(r'''
%%writefile nasa_parser.py
"""Parser log NASA (Common Log Format) yang dipakai bersama oleh mrjob dan Spark RDD."""
import re

# Grup: 1 = host, 2 = jam (00-23), 3 = status HTTP, 4 = bytes ("-" bila tidak ada).
# Bagian request memakai ".*" (greedy) karena sebagian URL memuat tanda kutip.
# Hanya memakai sintaks yang artinya sama di regex Python dan Java (dipakai Spark DataFrame).
LOG_PATTERN = (
    r'^(\S+) \S+ \S+ \[\d{2}/\w{3}/\d{4}:(\d{2}):\d{2}:\d{2} [^\]]*\] ".*" (\d{3}) (\d+|-)$'
)
_LOG_RE = re.compile(LOG_PATTERN)


def parse_line(line):
    """Kembalikan (host, jam, status, bytes), atau None bila baris tidak sesuai format."""
    match = _LOG_RE.match(line)
    if match is None:
        return None
    host, hour, status, nbytes = match.groups()
    return host, int(hour), int(status), 0 if nbytes == "-" else int(nbytes)
''')

code(r"""
import importlib

import nasa_parser
importlib.reload(nasa_parser)
from nasa_parser import LOG_PATTERN, parse_line

examples = [
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
    'frank.mtsu.edu - - [03/Jul/1995:02:41:15 -0400] "GET /images/" HTTP/1.0" 404 -',
    'alyssa.p',
]
for line in examples:
    print(f"{parse_line(line)!s:<45} <- {line[:70]}")

# Hitung baris valid/tidak valid sebagai acuan validasi di bagian 7.
with open(DATA_PATH) as f:
    invalid_lines = [line.rstrip("\n") for line in f if parse_line(line.rstrip("\n")) is None]
valid_lines = total_lines - len(invalid_lines)
print(f"\nBaris valid      : {valid_lines:,}")
print(f"Baris tidak valid: {len(invalid_lines):,} -> {invalid_lines[:5]}")
""")

# ---------------------------------------------------------------- 4. mrjob
md(r"""
## 4. Implementasi MapReduce (mrjob)

Satu skrip `mr_nasa.py` menangani ketiga analisis lewat argumen `--analysis`:

- **Mapper**: mem-parsing baris, lalu mengeluarkan pasangan key-value sesuai analisis.
- **Reducer**: menjumlahkan semua value untuk key yang sama.
- **Combiner** (opsional, `--use-combiner`): menjumlahkan sebagian di sisi Map *sebelum* shuffle.
  Contoh kode di spesifikasi tidak memakai combiner; di sini dijalankan versi **tanpa** (sesuai
  spesifikasi) dan **dengan** combiner, karena `reduceByKey` di Spark melakukannya otomatis.

Job dijalankan dengan runner **`local`**: mapper dan reducer berjalan sebagai beberapa subproses
paralel dengan tahap sort/shuffle di antaranya, meniru alur Hadoop di satu mesin. Ini **bukan**
cluster Hadoop sungguhan (tidak ada HDFS/YARN).
""")

code(r'''
%%writefile mr_nasa.py
"""Job MapReduce (mrjob) untuk tiga analisis log web server NASA."""
from mrjob.job import MRJob
from mrjob.step import MRStep

from nasa_parser import parse_line


class MRNasaLog(MRJob):
    # nasa_parser.py ikut disalin ke direktori kerja setiap task.
    FILES = ["nasa_parser.py"]

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--analysis", choices=["host", "status", "hour"], default="host")
        self.add_passthru_arg("--use-combiner", action="store_true")

    def mapper(self, _, line):
        record = parse_line(line)
        if record is None:
            return
        host, hour, status, nbytes = record
        if self.options.analysis == "host":
            yield host, 1
        elif self.options.analysis == "status":
            yield status, 1
        else:
            yield hour, nbytes

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner if self.options.use_combiner else None,
                reducer=self.reducer,
            )
        ]


if __name__ == "__main__":
    MRNasaLog.run()
''')

code(r"""
import logging

import mr_nasa
importlib.reload(mr_nasa)
from mr_nasa import MRNasaLog

# Sembunyikan peringatan "No configs specified for local runner" (memang tidak memakai mrjob.conf).
logging.getLogger("mrjob").setLevel(logging.ERROR)


def run_mrjob(analysis, path, combiner=False):
    # Jalankan satu job mrjob dan kembalikan hasilnya sebagai dict {key: total}.
    args = ["-r", "local", "--analysis", analysis, path]
    if combiner:
        args.append("--use-combiner")
    job = MRNasaLog(args=args)
    with job.make_runner() as runner:
        runner.run()
        return dict(job.parse_output(runner.cat_output()))
""")

# ---------------------------------------------------------------- 5. Spark RDD
md(r"""
## 5. Implementasi Apache Spark — RDD API

SparkSession dibuat sekali dengan `master("local[*]")` (semua inti CPU di mesin ini).
Waktu menyalakan Spark (JVM + SparkSession) dicatat terpisah karena hanya terjadi sekali,
tetapi bisa dominan pada data kecil.

Pada RDD, `parse_line` adalah fungsi Python, sehingga Spark harus menjalankan *Python worker*
dan memindahkan data bolak-balik antara JVM dan Python.
""")

code(r"""
import time
from operator import add

from pyspark.sql import SparkSession

start = time.perf_counter()
spark = (
    SparkSession.builder
    .appName("BatchProcessingBenchmarking")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
SPARK_STARTUP_S = time.perf_counter() - start

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sc.addPyFile("nasa_parser.py")
print(f"SparkSession siap dalam {SPARK_STARTUP_S:.2f} detik (Spark {spark.version}, {sc.defaultParallelism} inti)")
""")

code(r"""
def rdd_parsed(path):
    # Transformation saja (lazy): belum ada data yang dibaca.
    return sc.textFile(path).map(parse_line).filter(lambda rec: rec is not None)


def rdd_host(parsed):
    return dict(parsed.map(lambda rec: (rec[0], 1)).reduceByKey(add).collect())


def rdd_status(parsed):
    return dict(parsed.map(lambda rec: (rec[2], 1)).reduceByKey(add).collect())


def rdd_hour(parsed):
    return dict(parsed.map(lambda rec: (rec[1], rec[3])).reduceByKey(add).collect())


RDD_ANALYSES = {"host": rdd_host, "status": rdd_status, "hour": rdd_hour}


def run_rdd(analysis, path):
    # collect() adalah action: di sinilah Spark benar-benar menjalankan DAG.
    return RDD_ANALYSES[analysis](rdd_parsed(path))
""")

# ---------------------------------------------------------------- 6. Spark DataFrame
md(r"""
## 6. Implementasi Apache Spark — DataFrame API

Pola regex yang sama dijalankan di JVM lewat `rlike` dan `regexp_extract`, tanpa Python worker,
sehingga dapat dioptimalkan oleh Catalyst/Tungsten. Baris disaring dengan `rlike` lebih dulu agar
`cast` hanya dikenakan pada baris yang valid.
""")

code(r"""
from pyspark.sql import functions as F


def df_parsed(path):
    value = F.col("value")

    def field(group):
        return F.regexp_extract(value, LOG_PATTERN, group)

    return (
        spark.read.text(path)
        .filter(value.rlike(LOG_PATTERN))
        .select(
            field(1).alias("host"),
            field(2).cast("int").alias("hour"),
            field(3).cast("int").alias("status"),
            F.when(field(4) == "-", 0).otherwise(field(4).cast("long")).alias("bytes"),
        )
    )


def df_host(parsed):
    rows = parsed.groupBy("host").agg(F.count("*").alias("total_request")).collect()
    return {row["host"]: row["total_request"] for row in rows}


def df_status(parsed):
    rows = parsed.groupBy("status").agg(F.count("*").alias("total_request")).collect()
    return {row["status"]: row["total_request"] for row in rows}


def df_hour(parsed):
    rows = parsed.groupBy("hour").agg(F.sum("bytes").alias("total_bytes")).collect()
    return {row["hour"]: row["total_bytes"] for row in rows}


DF_ANALYSES = {"host": df_host, "status": df_status, "hour": df_hour}


def run_df(analysis, path):
    return DF_ANALYSES[analysis](df_parsed(path))
""")

code(r"""
# Rencana eksekusi (DAG) yang disusun Spark untuk analisis per host.
df_parsed(DATA_PATH).groupBy("host").count().explain()
""")

# ---------------------------------------------------------------- 7. Validasi
md(r"""
## 7. Validasi: Output Ketiga Pendekatan Identik

Setiap pendekatan dijalankan sekali pada data yang sama, lalu hasilnya dibandingkan key demi key.
Putaran ini sekaligus berfungsi sebagai **pemanasan** (*warm-up*): berkas data masuk ke cache
sistem operasi dan JVM sudah "panas", sehingga putaran benchmark berikutnya lebih adil.
Hasil putaran ini tidak dimasukkan ke benchmark.
""")

code(r"""
APPROACHES = {
    "MapReduce": lambda analysis: run_mrjob(analysis, DATA_PATH),
    "MapReduce + combiner": lambda analysis: run_mrjob(analysis, DATA_PATH, combiner=True),
    "Spark RDD": lambda analysis: run_rdd(analysis, DATA_PATH),
    "Spark DataFrame": lambda analysis: run_df(analysis, DATA_PATH),
}

outputs = {}
for analysis in ANALYSES:
    for name, run in APPROACHES.items():
        start = time.perf_counter()
        outputs[(name, analysis)] = run(analysis)
        print(f"{analysis:<7} {name:<22} selesai dalam {time.perf_counter() - start:6.1f} s")
""")

code(r"""
import pandas as pd

rows = []
for analysis in ANALYSES:
    reference = outputs[("MapReduce", analysis)]
    for name in APPROACHES:
        result = outputs[(name, analysis)]
        rows.append({
            "analisis": analysis,
            "pendekatan": name,
            "jumlah_key": len(result),
            "total_value": sum(result.values()),
            "identik_dgn_MapReduce": result == reference,
        })
validation = pd.DataFrame(rows)
display(validation)

assert validation["identik_dgn_MapReduce"].all(), "Ada output yang berbeda!"
# Jumlah request per host dan per status harus sama dengan jumlah baris valid (bagian 3).
for analysis in ["host", "status"]:
    assert sum(outputs[("MapReduce", analysis)].values()) == valid_lines
print("IDENTIK ✓  Ketiga pendekatan menghasilkan output yang sama untuk ketiga analisis.")
""")

# ---------------------------------------------------------------- 8. Alat ukur
md(r"""
## 8. Alat Ukur Benchmark

### 8.1 Fungsi dari spesifikasi (Tahap 4)

Fungsi `measure_execution` dari spesifikasi hanya mengukur proses Python notebook itu sendiri
(`os.getpid()`). Padahal Spark berjalan di proses **JVM** terpisah (beserta Python worker-nya)
dan mrjob runner `local` menjalankan **subproses**, sehingga RAM yang mereka pakai tidak terukur.
Contoh di bawah memperlihatkan hal itu.
""")

code(r"""
def measure_execution(func):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)
    start_time = time.time()

    func()

    end_time = time.time()
    mem_after = process.memory_info().rss / (1024 * 1024)
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    print(f"Memory Usage Delta: {mem_after - mem_before:.2f} MB")


measure_execution(lambda: run_df("host", DATA_PATH))
""")

md(r"""
### 8.2 Alat ukur yang diperbaiki

`ResourceMonitor` mengambil sampel setiap 0,1 detik dari proses notebook **beserta seluruh proses
anaknya** (JVM Spark, Python worker, subproses mrjob) dan mencatat:

| Metrik | Arti |
|---|---|
| `waktu_s` | Waktu eksekusi (*wall-clock*), diukur dengan `time.perf_counter()` |
| `cpu_persen` | Waktu CPU total ÷ waktu eksekusi × 100. Bisa > 100% bila memakai beberapa inti |
| `ram_puncak_mb` | RAM (RSS) tertinggi seluruh proses selama eksekusi |
| `ram_tambahan_mb` | `ram_puncak_mb` dikurangi RAM sebelum eksekusi dimulai |

JVM Spark tetap hidup sejak bagian 5, termasuk saat mrjob dijalankan. Agar RAM-nya tidak ikut
terhitung sebagai milik mrjob, pengukuran pendekatan MapReduce **mengecualikan proses Java beserta
turunannya** (`exclude_jvm=True`).

Keterbatasan: proses yang hidup kurang dari 0,1 detik bisa tidak tercatat, dan RSS proses-proses
yang berbagi memori bisa terhitung dua kali.
""")

code(r"""
import gc
import threading


class ResourceMonitor:
    # Mengukur waktu, CPU, dan RAM proses notebook beserta seluruh proses anaknya.

    def __init__(self, interval=0.1, exclude_jvm=False):
        self.interval = interval
        self.exclude_jvm = exclude_jvm
        self.root = psutil.Process(os.getpid())

    def _processes(self):
        procs = [self.root]
        try:
            for child in self.root.children():
                if self.exclude_jvm and child.name().startswith("java"):
                    continue
                procs += [child] + child.children(recursive=True)
        except psutil.Error:
            pass
        return procs

    def _sample(self):
        total_rss = 0
        for proc in self._processes():
            try:
                with proc.oneshot():
                    total_rss += proc.memory_info().rss
                    cpu = proc.cpu_times()
                    self._cpu_seconds[proc.pid] = cpu.user + cpu.system
            except psutil.Error:
                continue
        self._peak_rss = max(self._peak_rss, total_rss)
        return total_rss

    def _loop(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def __enter__(self):
        self._cpu_seconds = {}
        self._peak_rss = 0
        self.rss_before = self._sample()
        self._cpu_baseline = dict(self._cpu_seconds)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc_info):
        self.wall_s = time.perf_counter() - self._start
        self._stop.set()
        self._thread.join()
        self._sample()
        # Proses yang sudah ada sebelum pengukuran (misal JVM) dihitung selisihnya saja.
        cpu_s = sum(t - self._cpu_baseline.get(pid, 0.0) for pid, t in self._cpu_seconds.items())
        self.cpu_percent = 100 * cpu_s / self.wall_s
        self.peak_rss_mb = self._peak_rss / 1024**2
        self.extra_rss_mb = (self._peak_rss - self.rss_before) / 1024**2
        return False


records = []


def benchmark(scenario, approach, analysis, func, repeats=REPEATS):
    # Jalankan func sebanyak `repeats` kali dan catat metriknya ke `records`.
    for i in range(1, repeats + 1):
        gc.collect()
        with ResourceMonitor(exclude_jvm=approach.startswith("MapReduce")) as mon:
            func()
        records.append({
            "skenario": scenario,
            "pendekatan": approach,
            "analisis": analysis,
            "ulangan": i,
            "waktu_s": mon.wall_s,
            "cpu_persen": mon.cpu_percent,
            "ram_puncak_mb": mon.peak_rss_mb,
            "ram_tambahan_mb": mon.extra_rss_mb,
        })
        print(f"{approach:<26} {analysis:<7} #{i}: {mon.wall_s:7.2f} s | "
              f"CPU {mon.cpu_percent:5.0f}% | RAM puncak {mon.peak_rss_mb:7.0f} MB")
""")

# ---------------------------------------------------------------- 9. Benchmark
md(r"""
## 9. Benchmark

### Skenario 1 — setiap analisis dijalankan sendiri-sendiri

Setiap pendekatan membaca berkas dari awal, mem-parsing, lalu menjalankan satu agregasi.
""")

code(r"""
for analysis in ANALYSES:
    for name, run in APPROACHES.items():
        benchmark("1. per analisis", name, analysis, lambda: run(analysis))
""")

md(r"""
### Skenario 2 — ketiga analisis sekaligus pada dataset yang sama

- **MapReduce** harus menjalankan 3 job terpisah, sehingga berkas dibaca dan di-parsing 3 kali.
- **Spark tanpa cache** juga membaca ulang berkas untuk setiap action (3 kali).
- **Spark dengan `cache()`** membaca dan mem-parsing sekali, menyimpan hasilnya di memori,
  lalu ketiga agregasi memakai data dari memori.
""")

code(r"""
def mrjob_all(combiner):
    return {analysis: run_mrjob(analysis, DATA_PATH, combiner) for analysis in ANALYSES}


def spark_all(parse, analyses, cache):
    parsed = parse(DATA_PATH)
    if cache:
        parsed.cache()
    results = {analysis: analyses[analysis](parsed) for analysis in ANALYSES}
    if cache:
        parsed.unpersist()
    return results


ALL_AT_ONCE = {
    "MapReduce (3 job)": lambda: mrjob_all(combiner=False),
    "MapReduce + combiner (3 job)": lambda: mrjob_all(combiner=True),
    "Spark RDD tanpa cache": lambda: spark_all(rdd_parsed, RDD_ANALYSES, cache=False),
    "Spark RDD + cache": lambda: spark_all(rdd_parsed, RDD_ANALYSES, cache=True),
    "Spark DataFrame tanpa cache": lambda: spark_all(df_parsed, DF_ANALYSES, cache=False),
    "Spark DataFrame + cache": lambda: spark_all(df_parsed, DF_ANALYSES, cache=True),
}
for name, run in ALL_AT_ONCE.items():
    benchmark("2. tiga analisis sekaligus", name, "semua", run)
""")

# ---------------------------------------------------------------- 10. Hasil benchmark
md(r"""
## 10. Hasil Benchmark

Nilai yang ditampilkan adalah **rata-rata** dari `REPEATS` ulangan (± simpangan baku pada grafik).
Data mentah dan ringkasan disimpan di folder `hasil/` sebagai CSV, dan grafik sebagai PNG,
untuk dipakai di laporan.
""")

code(r"""
bench = pd.DataFrame(records)
bench.to_csv(f"{RESULT_DIR}/benchmark_mentah.csv", index=False)

METRICS = ["waktu_s", "cpu_persen", "ram_puncak_mb", "ram_tambahan_mb"]
summary = (
    bench.groupby(["skenario", "analisis", "pendekatan"], sort=False)[METRICS]
    .agg(["mean", "std"])
    .round(2)
)
summary.to_csv(f"{RESULT_DIR}/benchmark_ringkasan.csv")
print(f"Waktu menyalakan SparkSession: {SPARK_STARTUP_S:.2f} s (sekali di awal, tidak termasuk di tabel)")
display(summary)
""")

code(r"""
import matplotlib.pyplot as plt

METRIC_LABELS = {
    "waktu_s": "Waktu eksekusi (detik)",
    "cpu_persen": "CPU (%)",
    "ram_puncak_mb": "RAM puncak (MB)",
}
scenario1 = bench[bench["skenario"] == "1. per analisis"]
approach_names = list(APPROACHES)

fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))
for ax, (metric, label) in zip(axes, METRIC_LABELS.items()):
    stats = scenario1.groupby(["analisis", "pendekatan"])[metric].agg(["mean", "std"])
    width = 0.8 / len(approach_names)
    for i, name in enumerate(approach_names):
        means = [stats.loc[(a, name), "mean"] for a in ANALYSES]
        stds = [stats.loc[(a, name), "std"] for a in ANALYSES]
        positions = [x + i * width for x in range(len(ANALYSES))]
        ax.bar(positions, means, width, yerr=stds, capsize=3, label=name)
    ax.set_xticks([x + width * (len(approach_names) - 1) / 2 for x in range(len(ANALYSES))])
    ax.set_xticklabels(ANALYSES)
    ax.set_title(label)
    ax.grid(axis="y", alpha=0.3)
axes[0].legend(fontsize=8)
fig.suptitle("Skenario 1: setiap analisis dijalankan sendiri-sendiri")
fig.tight_layout()
fig.savefig(f"{RESULT_DIR}/skenario1.png", dpi=150)
plt.show()
""")

code(r"""
scenario2 = bench[bench["skenario"] == "2. tiga analisis sekaligus"]
stats2 = scenario2.groupby("pendekatan", sort=False)[list(METRIC_LABELS)].agg(["mean", "std"])

fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))
for ax, (metric, label) in zip(axes, METRIC_LABELS.items()):
    ax.barh(stats2.index, stats2[(metric, "mean")], xerr=stats2[(metric, "std")], capsize=3)
    ax.invert_yaxis()
    ax.set_title(label)
    ax.grid(axis="x", alpha=0.3)
for ax in axes[1:]:
    ax.set_yticklabels([])
fig.suptitle("Skenario 2: ketiga analisis sekaligus (pengaruh caching)")
fig.tight_layout()
fig.savefig(f"{RESULT_DIR}/skenario2.png", dpi=150)
plt.show()
""")

code(r"""
# Perbandingan waktu terhadap MapReduce versi spesifikasi (tanpa combiner).
mean_time = scenario1.groupby(["analisis", "pendekatan"])["waktu_s"].mean().unstack().reindex(ANALYSES)
speedup = mean_time.rdiv(mean_time["MapReduce"], axis=0).round(2)
print("Berapa kali lebih cepat dibanding 'MapReduce' (nilai > 1 = lebih cepat):")
display(speedup[approach_names])

mean_time2 = scenario2.groupby("pendekatan", sort=False)["waktu_s"].mean()
print("\nSkenario 2, berapa kali lebih cepat dibanding 'MapReduce (3 job)':")
display((mean_time2["MapReduce (3 job)"] / mean_time2).round(2).to_frame("kali_lebih_cepat"))
""")

# ---------------------------------------------------------------- 11. Hasil analisis data
md(r"""
## 11. Hasil Analisis Data

Hasil diambil dari output Spark DataFrame (sudah terbukti identik dengan pendekatan lain di bagian 7).
""")

code(r"""
host_counts = pd.Series(outputs[("Spark DataFrame", "host")]).sort_values(ascending=False)
status_counts = pd.Series(outputs[("Spark DataFrame", "status")]).sort_index()
hour_bytes = pd.Series(outputs[("Spark DataFrame", "hour")]).sort_index()

host_counts.head(10).to_frame("total_request").to_csv(f"{RESULT_DIR}/top10_host.csv")
status_counts.to_frame("total_request").to_csv(f"{RESULT_DIR}/status_code.csv")
(hour_bytes / 1024**2).round(1).to_frame("total_MB").to_csv(f"{RESULT_DIR}/traffic_per_jam.csv")

print(f"Jumlah host unik: {len(host_counts):,}")
display(host_counts.head(10).to_frame("total_request"))
display(status_counts.to_frame("total_request").assign(
    persen=lambda d: (100 * d["total_request"] / d["total_request"].sum()).round(3)))
""")

code(r"""
fig, axes = plt.subplots(1, 3, figsize=(17, 4.5))

host_counts.head(10).sort_values().plot.barh(ax=axes[0])
axes[0].set_title("10 host dengan request terbanyak")
axes[0].set_xlabel("Jumlah request")

status_counts.plot.bar(ax=axes[1], logy=True)
axes[1].set_title("Distribusi HTTP status code (skala log)")
axes[1].set_xlabel("Status code")
axes[1].set_ylabel("Jumlah request")

(hour_bytes / 1024**3).plot.bar(ax=axes[2])
axes[2].set_title("Total traffic per jam-dalam-sehari")
axes[2].set_xlabel("Jam (waktu server, UTC-4)")
axes[2].set_ylabel("GB")

for ax in axes:
    ax.grid(axis="x" if ax is axes[0] else "y", alpha=0.3)
fig.tight_layout()
fig.savefig(f"{RESULT_DIR}/hasil_analisis.png", dpi=150)
plt.show()
""")

# ---------------------------------------------------------------- 12. Catatan
md(r"""
## 12. Catatan untuk Laporan

**Keterbatasan eksperimen** (perlu disebut di Bab 1 dan Bab 3):

- Semua pendekatan berjalan di **satu mesin** (Google Colab), bukan cluster. Paralelisme berasal
  dari beberapa inti CPU; tidak ada HDFS, YARN, maupun biaya jaringan saat shuffle.
- "MapReduce" di sini adalah **mrjob runner `local` (Python murni)** yang meniru alur Hadoop,
  bukan Hadoop MapReduce (Java) di cluster.
- Ukuran data (±356 MB) masih muat di RAM satu mesin, sehingga keunggulan pemrosesan
  terdistribusi belum terlihat sepenuhnya dan biaya awal (misal menyalakan JVM) bisa dominan.
- Spesifikasi mesin Colab bisa berbeda antar-sesi; spesifikasi saat pengukuran tercatat di bagian 1.

**Pertanyaan pemandu untuk Bab 3** (jawab berdasarkan angka di bagian 10):

1. Seberapa besar selisih waktu MapReduce vs Spark? Apakah sesuai dugaan "Spark lebih cepat"?
2. Seberapa besar pengaruh combiner pada MapReduce, dan pada analisis mana paling terasa
   (petunjuk: analisis `status` hanya punya sedikit key)?
3. Mengapa Spark RDD (fungsi Python) dan Spark DataFrame (fungsi bawaan JVM) berbeda kecepatannya?
4. Pada skenario 2, berapa penghematan waktu yang diberikan `cache()`, dan berapa biaya RAM-nya?
5. Bagaimana pola CPU % dan RAM tiap pendekatan, dan apa penyebabnya (JVM, subproses, GC)?
""")

code(r"""
spark.stop()
""")


def main():
    nb = new_notebook(cells=CELLS)
    nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    nb.metadata["language_info"] = {"name": "python"}
    nbformat.validate(nb)
    nbformat.write(nb, sys.argv[1])
    print(f"{len(CELLS)} sel ditulis ke {sys.argv[1]}")


if __name__ == "__main__":
    main()
