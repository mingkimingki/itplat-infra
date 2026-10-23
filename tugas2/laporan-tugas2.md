> **DRAFT** — hapus kotak ini sebelum dijadikan PDF. Semua angka benchmark berasal dari eksekusi
> notebook di Google Colab (`hasil/*.csv`). Angka "uji pendukung" (subbab 3.2 dan 3.4) berasal dari
> komputer anggota kelompok dan ditandai jelas. Bagian bertanda **[ISI]** perlu dilengkapi.

# Laporan Tugas 2
## Implementasi Pipeline Pemrosesan Batch dengan Hadoop MapReduce dan Apache Spark

**Mata Kuliah:** Pemrosesan Data Terdistribusi / Big Data Analytics
**Dosen Pengampu:** [ISI]
**Kelompok 21**

| No | Nama | NIM |
|---|---|---|
| 1 | Eka Putra Kurnia | 25.55.2927 |
| 2 | Zahrul Amri Hutabarat | 25.55.2900 |
| 3 | Bujur Depoin Sianipar | 25.55.2879 |
| 4 | Riyan Pratomo Saputra | 25.55.2857 |

**Tanggal:** [ISI]

---

## Bab 1. Pendahuluan

### 1.1 Latar Belakang dan Tujuan

Pemrosesan *batch* merupakan fondasi analisis data berukuran besar secara terdistribusi. Hadoop
MapReduce memperkenalkan model komputasi dua tahap (Map dan Reduce) yang menyimpan hasil antara di
disk, sedangkan Apache Spark menyusun pekerjaan sebagai *Directed Acyclic Graph* (DAG) dan
mengutamakan pemrosesan di memori.

Eksperimen ini bertujuan membandingkan kedua kerangka kerja secara kuantitatif dan kualitatif,
yaitu: (1) mengimplementasikan pipeline yang sama dengan tiga pendekatan — MapReduce (mrjob),
Spark RDD API, dan Spark DataFrame API; (2) membuktikan bahwa ketiganya menghasilkan output
identik; (3) mengukur waktu eksekusi, penggunaan CPU, dan RAM; serta (4) menjelaskan penyebab
perbedaan performa dari sisi arsitektur masing-masing kerangka kerja.

### 1.2 Arsitektur Data

Dalam arsitektur Big Data yang umumnya berlapis (sumber data → *ingestion* → *storage* →
*processing* → *serving*), eksperimen ini berfokus pada **lapisan processing jalur batch**.
Alur data yang diimplementasikan:

```
 Sumber data            Ingestion            Storage                 Processing (3 pendekatan)          Serving
 ───────────            ─────────            ───────                 ─────────────────────────          ───────
 Log server NASA  ──►  unduh .gz dari  ──►  1 berkas teks     ──┬──► MapReduce (mrjob, runner local) ──┐
 (Jul + Agu 1995)      arsip LBL,           356 MB di disk      ├──► Spark RDD API      (local[*])   ──┼──► tabel, grafik,
                       ekstrak,             lokal Colab         └──► Spark DataFrame API (local[*])   ──┘    berkas CSV
                       normalisasi          (pengganti HDFS)
```

Seluruh pemrosesan berjalan di **satu mesin Google Colab**. Paralelisme berasal dari beberapa inti
CPU, bukan dari beberapa komputer, sehingga eksperimen ini merupakan **simulasi pemrosesan
terdistribusi di satu mesin**. Tidak ada HDFS, YARN, maupun lalu lintas jaringan antar-node.
Konsekuensinya dibahas di subbab 3.7.

### 1.3 Dataset

Dataset yang digunakan adalah **NASA HTTP Access Logs** dari *Internet Traffic Archive* (LBL):
log server web NASA Kennedy Space Center periode 1 Juli – 31 Agustus 1995. Setelah digabung,
berkas berukuran **355,8 MB** (373 juta byte) dengan **3.461.613 baris**. Setiap baris mengikuti
*Common Log Format*:

```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
   host                 waktu                      request                  status bytes
```

Persiapan data yang dilakukan:

- **Normalisasi karakter.** 18 baris memuat byte non-ASCII atau karakter kontrol. Karena regex
  Python (mrjob, RDD) dan regex Java (DataFrame) memaknai karakter tersebut sedikit berbeda, setiap
  byte di luar ASCII yang dapat dicetak diganti dengan `?` agar ketiga pendekatan membaca teks yang
  persis sama.
- **Parser bersama.** Satu pola regex dipakai oleh ketiga pendekatan untuk mengambil host, jam,
  status, dan bytes. Bagian request memakai `".*"` (*greedy*) karena 33 baris memuat tanda kutip di
  dalam URL. Hanya **1 baris** yang tidak valid (terpotong: `alyssa.p`) dan dibuang secara
  konsisten, sehingga tersisa **3.461.612 request valid** — sama dengan jumlah yang dilaporkan
  sumber dataset.
- Nilai bytes `-` (tidak ada data yang dikirim) dianggap 0.

### 1.4 Desain Eksperimen

**Tiga analisis batch** sesuai studi kasus Log Web Server:

| Kode | Analisis | Key → value | Jumlah key |
|---|---|---|---|
| `host` | Jumlah request per host | host → 1, dijumlah | 137.978 |
| `status` | Distribusi HTTP status code | status → 1, dijumlah | 8 |
| `hour` | Total traffic per jam-dalam-sehari | jam → bytes, dijumlah | 24 |

**Empat pendekatan:** MapReduce tanpa combiner (sesuai contoh kode spesifikasi), MapReduce dengan
combiner, Spark RDD, dan Spark DataFrame.

**Dua skenario:**
1. *Per analisis* — setiap analisis dijalankan terpisah; setiap pendekatan membaca berkas dari awal.
2. *Tiga analisis sekaligus* — ketiga analisis dijalankan berurutan pada dataset yang sama, untuk
   Spark dengan dan tanpa `cache()`.

**Metrik dan metodologi:**
- Waktu eksekusi (*wall-clock*, `time.perf_counter()`), CPU % (waktu CPU ÷ waktu eksekusi, dapat
  melebihi 100% pada mesin multi-inti), dan RAM puncak (RSS).
- Pengukuran mencakup proses notebook **beserta seluruh proses anaknya** (JVM Spark, Python worker,
  subproses mrjob), diambil setiap 0,1 detik. Untuk MapReduce, proses JVM Spark yang sedang
  menganggur dikecualikan agar tidak terhitung sebagai milik mrjob.
- Sebelum pengukuran, setiap pendekatan dijalankan sekali sebagai **pemanasan** (sekaligus validasi
  output); hasil pemanasan tidak dimasukkan. Setiap pengukuran diulang **3 kali** lalu dirata-rata.
- Waktu menyalakan SparkSession dicatat terpisah karena hanya terjadi sekali.

**Lingkungan:** Google Colab — 2 inti CPU (logical), RAM 12,7 GB, Python 3.13.16, PySpark 4.0.4,
Java OpenJDK 17, mrjob 0.7.4. Spesifikasi meminta Java 8, tetapi PySpark 4.x membutuhkan Java 17
atau lebih baru, sehingga digunakan Java 17.

---

## Bab 2. Hasil dan Benchmarking

### 2.1 Validasi Output

Ketiga pendekatan (empat varian) menghasilkan output **identik** untuk ketiga analisis, dibuktikan
dengan perbandingan otomatis key demi key di notebook (bagian 7):

| Analisis | Jumlah key | Total value | MapReduce | MR + combiner | Spark RDD | Spark DataFrame |
|---|---|---|---|---|---|---|
| host | 137.978 | 3.461.612 request | ✓ | ✓ | ✓ | ✓ |
| status | 8 | 3.461.612 request | ✓ | ✓ | ✓ | ✓ |
| hour | 24 | 65.524.314.915 byte | ✓ | ✓ | ✓ | ✓ |

### 2.2 Skenario 1: Setiap Analisis Dijalankan Terpisah

Tabel 2.1 — Rata-rata 3 ulangan (± simpangan baku). CPU maksimum teoretis 200% (2 inti).

| Analisis | Pendekatan | Waktu (detik) | CPU (%) | RAM puncak (MB) |
|---|---|---|---|---|
| host | MapReduce | 65,20 ± 1,28 | 154 | 888 |
| | MapReduce + combiner | 68,17 ± 2,93 | 154 | 953 |
| | Spark RDD | 20,60 ± 1,03 | 167 | 1.476 |
| | Spark DataFrame | **17,90 ± 1,41** | 152 | 1.781 |
| status | MapReduce | 66,19 ± 2,21 | 149 | 903 |
| | MapReduce + combiner | 68,83 ± 3,84 | 157 | 941 |
| | Spark RDD | 19,15 ± 0,91 | 164 | 1.494 |
| | Spark DataFrame | **15,99 ± 0,72** | 153 | 1.644 |
| hour | MapReduce | 69,62 ± 2,16 | 156 | 901 |
| | MapReduce + combiner | 69,86 ± 4,70 | 157 | 940 |
| | Spark RDD | **19,80 ± 0,54** | 165 | 1.249 |
| | Spark DataFrame | 31,19 ± 0,53 | 155 | 1.458 |

![Skenario 1](hasil/skenario1.png)

*Gambar 2.1 — Waktu eksekusi, CPU, dan RAM puncak skenario 1.*

Tabel 2.2 — Kecepatan relatif terhadap MapReduce tanpa combiner (nilai > 1 = lebih cepat).

| Analisis | MR + combiner | Spark RDD | Spark DataFrame |
|---|---|---|---|
| host | 0,96× | 3,16× | **3,64×** |
| status | 0,96× | 3,46× | **4,14×** |
| hour | 1,00× | **3,52×** | 2,23× |

Waktu menyalakan SparkSession (JVM + konteks Spark) adalah **26,13 detik**, terjadi sekali di awal
dan tidak termasuk dalam tabel di atas.

### 2.3 Skenario 2: Tiga Analisis Sekaligus

Tabel 2.3 — Rata-rata 3 ulangan.

| Pendekatan | Waktu (detik) | CPU (%) | RAM puncak (MB) | Kecepatan relatif |
|---|---|---|---|---|
| MapReduce (3 job) | 203,74 ± 5,20 | 152 | 897 | 1,00× |
| MapReduce + combiner (3 job) | 202,07 ± 4,33 | 158 | 959 | 1,01× |
| Spark RDD tanpa cache | 58,19 ± 1,51 | 164 | 1.480 | 3,50× |
| Spark RDD + cache | **34,38 ± 1,29** | 163 | 1.634 | **5,93×** |
| Spark DataFrame tanpa cache | 65,95 ± 4,28 | 151 | 1.977 | 3,09× |
| Spark DataFrame + cache | 57,74 ± 2,27 | 153 | 1.714 | 3,53× |

![Skenario 2](hasil/skenario2.png)

*Gambar 2.2 — Skenario 2: pengaruh caching pada tiga analisis sekaligus.*

### 2.4 Hasil Analisis Data

- **Request per host:** terdapat 137.978 host unik. Host teratas adalah `piweba3y.prodigy.com`
  (21.988 request), disusul `piweba4y.prodigy.com` (16.437) dan `piweba1y.prodigy.com` (12.825) —
  ketiganya server *proxy* penyedia layanan Prodigy, sehingga mewakili banyak pengguna sekaligus.
- **Status code:** 89,57% request berhasil (200), 7,71% *not modified* (304), 2,11% *redirect*
  (302), 0,60% *not found* (404), dan hanya 65 request (0,002%) *server error* (500).
- **Traffic per jam:** total 65,5 miliar byte (±61 GB). Jam tersibuk adalah **13.00–13.59**
  (±4.066 MB) dengan rentang jam 12–16 hampir sama padatnya; jam tersepi **05.00–05.59**
  (±1.132 MB), sekitar 3,6 kali lebih rendah (waktu server, UTC−4).

![Hasil analisis data](hasil/hasil_analisis.png)

*Gambar 2.3 — 10 host teratas, distribusi status code (skala log), dan traffic per jam.*

---

## Bab 3. Analisis Komparatif

### 3.1 I/O Disk vs Eksekusi di Memori

Pada skenario 1, Spark 2,2–4,1 kali lebih cepat daripada MapReduce. Perbedaan alur data menjadi
penyebab utamanya:

- **MapReduce (mrjob runner `local`)** menjalankan mapper sebagai subproses Python yang menulis
  setiap pasangan key-value ke **berkas antara di disk**, mengurutkannya, lalu reducer membaca
  kembali berkas tersebut. Setiap pasangan juga diubah ke teks JSON saat ditulis dan diurai kembali
  saat dibaca.
- **Spark** menjalankan langkah-langkah yang tidak memerlukan shuffle (baca → saring → ekstrak →
  agregasi parsial) secara **berantai di memori** (*pipelining*) di dalam satu *stage*, tanpa
  menyimpan hasil antara. Data baru ditulis ke disk lokal saat shuffle, dan saat itu volumenya
  sudah sangat kecil karena telah diagregasi sebagian.

Keunggulan memori paling jelas terlihat pada **skenario 2**. MapReduce membutuhkan 203,7 detik,
hampir tepat 3 kali waktu satu job (±67 detik), karena setiap job membaca dan mem-parsing ulang
berkas 356 MB dari disk. Spark tanpa cache mengalami hal serupa (58,2 detik, mendekati 3 × 19,9 detik)
karena setiap *action* menjalankan ulang DAG dari awal. Dengan `cache()`, Spark RDD hanya
membutuhkan **34,4 detik (1,69 kali lebih cepat)**: data hasil parsing disimpan di memori pada
action pertama, sehingga dua analisis berikutnya tidak perlu membaca dan mem-parsing berkas lagi.
Jika action pertama memakan ±20 detik seperti di skenario 1, dua action berikutnya hanya ±7 detik
masing-masing.

Pada DataFrame, cache hanya mempercepat 1,14 kali (66,0 → 57,7 detik). Penyebabnya, sebagian besar
biaya DataFrame dalam implementasi ini terletak pada evaluasi regex di tahap ekstraksi (lihat
subbab 3.4), dan sebagian evaluasi itu tetap terjadi saat data pertama kali dimasukkan ke cache.

### 3.2 Biaya Shuffle dan Peran Combiner

Shuffle adalah tahap mengumpulkan semua pasangan dengan key yang sama ke reducer yang sama. Tanpa
combiner, seluruh 3.461.612 pasangan `(key, nilai)` harus melewati tahap ini. *Combiner* menjumlah
sebagian di sisi map sebelum shuffle; pada analisis `status` yang hanya memiliki 8 key, jumlah
pasangan yang di-shuffle turun dari jutaan menjadi beberapa puluh saja.

Namun hasil di Colab menunjukkan combiner **tidak mempercepat** MapReduce (0,96–1,00 kali).
Selisihnya (±1–3 detik) masih berada dalam rentang simpangan baku (hingga 4,7 detik), sehingga
dapat dianggap tidak berbeda.

Untuk mencari penyebabnya, dilakukan **uji pendukung** di komputer anggota kelompok (8 inti,
mrjob dibatasi 2 inti agar menyerupai Colab; angka absolut berbeda dari Colab, yang dibandingkan
adalah proporsinya):

| Uji pendukung (komputer anggota) | Waktu |
|---|---|
| Membaca seluruh berkas saja (1 proses Python) | 0,3 s |
| Membaca + mem-parsing seluruh baris dengan regex (1 proses Python) | 4,3 s |
| Job mrjob `host`, tanpa / dengan combiner (2 inti) | 17,9 s / 16,4 s |
| Job mrjob `status`, tanpa / dengan combiner (2 inti) | 19,4 s / 16,5 s |

Parsing hanya menyumbang sebagian kecil waktu job. Sebagian besar waktu mrjob habis untuk
**biaya kerangka kerja**: menjalankan subproses untuk setiap task, serialisasi JSON setiap
pasangan key-value, menulis dan membaca berkas antara, serta pengurutan. Combiner memang mengurangi
data yang di-shuffle, tetapi di mrjob ia juga menambah langkah pengurutan dan pemrosesan tersendiri
di setiap task map. Keuntungan bersihnya kecil (±8–15% di komputer uji) dan tidak terlihat di Colab
yang hanya memiliki 2 inti.

Hal ini berbeda dengan Hadoop di cluster sungguhan, tempat shuffle melibatkan **pengiriman data
lewat jaringan** antar-node. Di sana pengurangan data oleh combiner jauh lebih berharga. Pada
eksperimen satu mesin ini, biaya jaringan tersebut tidak ada.

Spark menerapkan prinsip yang sama secara otomatis. Rencana eksekusi DataFrame menunjukkan
`HashAggregate(... partial_count(1))` **sebelum** `Exchange hashpartitioning(...)` (shuffle),
yang berarti agregasi parsial dilakukan sebelum data di-shuffle. Pada RDD, `reduceByKey`
melakukan hal serupa (*map-side combine*).

### 3.3 Pembuatan DAG vs Pembagian Map-Reduce

**MapReduce** memaksa setiap pekerjaan berbentuk tetap: satu fungsi map, shuffle, satu fungsi
reduce. Tiga analisis berarti tiga job terpisah, masing-masing membaca input dari awal. Pekerjaan
yang lebih kompleks harus dirangkai sebagai beberapa job, dengan hasil antara ditulis ke disk di
antara job.

**Spark** mencatat setiap transformasi (`map`, `filter`, `select`, `groupBy`) tanpa langsung
menjalankannya (*lazy evaluation*). Saat *action* seperti `collect()` dipanggil, seluruh rangkaian
disusun menjadi DAG, dioptimalkan, lalu dipecah menjadi *stage* dengan shuffle sebagai batasnya.
Rencana fisik analisis `host` dari notebook memperlihatkan hal ini:

```
HashAggregate(keys=[host], functions=[count(1)])             ← stage 2: agregasi akhir
+- Exchange hashpartitioning(host, 200)                      ← shuffle (batas stage)
   +- HashAggregate(keys=[host], functions=[partial_count(1)]) ← stage 1: agregasi parsial
      +- Project [regexp_extract(value, ..., 1) AS host]
         +- Filter (RLIKE(value, ...))
            +- FileScan text [value]
```

Seluruh langkah di stage 1 dijalankan berantai per baris dalam satu task, tanpa hasil antara di
disk. Pengoptimal Catalyst juga secara otomatis menyisipkan agregasi parsial. Fleksibilitas DAG ini
pula yang memungkinkan `cache()` di skenario 2: Spark dapat "memotong" DAG di titik data yang
di-cache, sesuatu yang tidak dapat dilakukan MapReduce antar-job.

### 3.4 Spark RDD vs Spark DataFrame

Pada analisis `host` dan `status`, DataFrame 13–17% lebih cepat daripada RDD. Pada RDD, fungsi
parsing adalah fungsi Python (`lambda` dan `parse_line`), sehingga Spark harus menjalankan
**Python worker** dan memindahkan setiap baris bolak-balik antara JVM dan Python (serialisasi).
DataFrame memakai fungsi bawaan (`rlike`, `regexp_extract`, `groupBy`) yang dieksekusi langsung di
JVM dan dioptimalkan oleh Catalyst dan Tungsten.

Sebaliknya, pada analisis `hour` DataFrame justru **58% lebih lambat** daripada RDD (31,2 vs 19,8
detik). Uji pendukung (komputer anggota, Spark `local[2]`) menunjukkan penyebabnya adalah jumlah
evaluasi regex per baris:

| Uji pendukung DataFrame (komputer anggota) | Waktu |
|---|---|
| `rlike` + 1 `regexp_extract` | 5,5 s |
| `rlike` + 2 `regexp_extract` | 8,0 s |
| `rlike` + 3 `regexp_extract` | 10,7 s |
| `rlike` + 4 `regexp_extract` | 13,1 s |
| Analisis `host` versi notebook (rlike + 1 extract) | 6,3 s |
| Analisis `hour` versi notebook (rlike + extract jam + extract bytes) | 10,6 s |
| Analisis `hour`, bytes diambil tanpa regex (`substring_index`) | 5,8 s |

Setiap `regexp_extract` menjalankan ulang regex yang sama pada baris yang sama (±2,6 detik per
tambahan). Analisis `hour` mengambil dua kolom (jam dan bytes), dan rencana eksekusinya
memperlihatkan ekstraksi bytes muncul dua kali (di kondisi `CASE WHEN` dan di `cast`), sehingga
waktunya setara dengan tiga kali ekstraksi. Ketika bytes diambil tanpa regex, waktunya turun ke
level analisis `host`. Sementara itu, RDD mem-parsing setiap baris **sekali** dan langsung
menghasilkan keempat kolom.

Temuan ini menunjukkan bahwa DataFrame tidak otomatis lebih cepat: performanya bergantung pada
cara ekspresi ditulis, dan pengoptimal tidak menghilangkan evaluasi regex yang berulang dalam kasus
ini. Implementasi `hour` di notebook bukan bentuk paling efisien; hal ini dicatat sebagai
keterbatasan, dan perbaikannya (mengambil bytes tanpa regex) diperkirakan menyamakan waktunya
dengan analisis lain.

### 3.5 Penggunaan Sumber Daya

**CPU.** Semua pendekatan memakai 149–167% dari maksimum 200%, artinya kedua inti hampir selalu
sibuk. Spark RDD paling tinggi (164–167%) karena JVM dan Python worker bekerja bersamaan. Pada mesin
2 inti, perbedaan kecepatan **tidak** berasal dari pemanfaatan inti yang lebih banyak, melainkan
dari jumlah pekerjaan yang lebih sedikit per baris data.

**RAM.** MapReduce memakai ±0,9 GB dan sangat stabil (simpangan baku < 1 MB tanpa combiner),
karena setiap subproses mrjob hanya memegang sebagian kecil data dan sisanya berada di berkas.
Spark memakai **1,25–1,98 GB, sekitar 1,4–2,2 kali MapReduce**. JVM mengalokasikan *heap* (diatur
4 GB untuk driver) dan jarang mengembalikan memori ke sistem operasi; besar RSS-nya bergantung pada
kapan *garbage collector* (GC) berjalan. Hal ini terlihat dari simpangan baku RAM Spark yang besar
(hingga 270 MB). Karena itu, perbedaan RAM antar-varian Spark (misalnya DataFrame dengan dan tanpa
cache) tidak dapat ditafsirkan secara tegas dari tiga ulangan.

**Biaya awal Spark.** Menyalakan SparkSession memakan **26,1 detik**, lebih lama daripada satu
analisis Spark itu sendiri (16–21 detik). Meski demikian, jika biaya awal ini dimasukkan, Spark
DataFrame untuk analisis `host` (26,1 + 17,9 = 44,0 detik) tetap lebih cepat daripada MapReduce
(65,2 detik). Efek pemanasan juga terlihat: eksekusi pertama Spark RDD untuk `host` saat validasi
membutuhkan 27,8 detik, sedangkan rata-rata saat benchmark 20,6 detik.

**Keterbatasan alat ukur spesifikasi.** Fungsi `measure_execution` pada spesifikasi hanya mengukur
proses Python notebook (`os.getpid()`). Saat dipakai mengukur Spark DataFrame, fungsi ini
melaporkan selisih memori hanya **7,64 MB**, padahal JVM Spark memakai lebih dari 1 GB. Oleh karena
itu eksperimen ini memakai alat ukur yang menyertakan seluruh proses anak.

### 3.6 Kompleksitas Pengembangan Kode

| Aspek | MapReduce (mrjob) | Spark RDD | Spark DataFrame |
|---|---|---|---|
| Baris kode inti (tanpa komentar) | ±35 (+13 pembungkus runner) | ±11 | ±27 |
| Gaya pemrograman | Kelas dengan method `mapper`/`combiner`/`reducer`, berkas skrip terpisah | Fungsional (`map`, `reduceByKey`) | Deklaratif mirip SQL (`select`, `groupBy`, `agg`) |
| Menambah analisis | Opsi baru atau job baru | Satu fungsi pendek | Satu fungsi pendek |
| Kemudahan debugging | Rendah: berjalan di subproses, log tersebar | Sedang | Sedang: `explain()` memperlihatkan rencana eksekusi |

RDD paling ringkas karena dapat memakai ulang fungsi `parse_line` secara langsung. DataFrame
sedikit lebih panjang karena setiap kolom diekstraksi dengan ekspresi tersendiri, tetapi kodenya
deklaratif dan rencana eksekusinya dapat diperiksa. mrjob paling panjang dan kaku karena setiap
pekerjaan harus dinyatakan dalam bentuk mapper–reducer.

### 3.7 Keterbatasan Eksperimen

1. Semua pendekatan berjalan di **satu mesin Colab dengan 2 inti**, bukan cluster. Biaya jaringan
   saat shuffle, koordinasi antar-node, dan toleransi kegagalan tidak terwakili.
2. "MapReduce" di sini adalah **mrjob runner `local` (Python murni)** yang meniru alur Hadoop,
   bukan Hadoop MapReduce (Java) di atas HDFS dan YARN.
3. Dataset (356 MB) masih muat di RAM satu mesin, sehingga keunggulan pemrosesan terdistribusi
   untuk data yang jauh melebihi kapasitas satu mesin belum teramati.
4. Implementasi DataFrame untuk analisis `hour` belum optimal (subbab 3.4).
5. Pengukuran RAM berbasis RSS dengan sampel setiap 0,1 detik: proses yang sangat singkat bisa
   tidak tercatat, dan memori bersama antar-proses dapat terhitung ganda.
6. Uji pendukung dilakukan di komputer berbeda (bukan Colab); yang dibandingkan adalah proporsi,
   bukan angka absolutnya.

---

## Bab 4. Kesimpulan

1. Ketiga pendekatan berhasil diimplementasikan dan menghasilkan output **identik** pada 3,46 juta
   request log NASA.
2. Pada lingkungan Colab 2 inti, **Spark 2,2–4,1 kali lebih cepat** daripada MapReduce (mrjob)
   untuk setiap analisis, dan **hingga 5,9 kali lebih cepat** untuk tiga analisis sekaligus dengan
   `cache()`. Sebagai gantinya, Spark memakai **RAM 1,4–2,2 kali lebih besar** dan membutuhkan
   ±26 detik untuk dinyalakan.
3. Keunggulan Spark berasal dari eksekusi berantai di memori dalam DAG, agregasi parsial otomatis
   sebelum shuffle, dan kemampuan menyimpan data di memori untuk dipakai ulang. Kelemahan MapReduce
   berasal dari biaya kerangka kerja (subproses, serialisasi, berkas antara), bukan dari parsing.
4. Combiner tidak memberi keuntungan berarti pada eksperimen satu mesin ini; manfaatnya diperkirakan
   lebih besar di cluster sungguhan, tempat shuffle melibatkan jaringan.
5. DataFrame umumnya lebih cepat daripada RDD, tetapi tidak selalu: cara menulis ekspresi (misalnya
   regex yang dievaluasi berulang) dapat membalik hasilnya.

**Rekomendasi pemilihan kerangka kerja pada kasus nyata:**

| Situasi | Rekomendasi | Alasan |
|---|---|---|
| Analitik data terstruktur/semi-terstruktur, laporan rutin, query mirip SQL | **Spark DataFrame** | Ringkas, dioptimalkan otomatis; perhatikan ekspresi yang mahal |
| Beberapa analisis atau algoritma iteratif pada data yang sama | **Spark + `cache()`** | Data dibaca dan di-parse sekali, dipakai berulang dari memori |
| Logika pemrosesan khusus yang sulit dinyatakan sebagai operasi kolom | **Spark RDD** | Fleksibel memakai fungsi Python bebas |
| Data sangat besar melebihi kapasitas memori cluster, job batch sekali jalan, cluster Hadoop sudah tersedia | **Hadoop MapReduce** | Hemat memori dan stabil; berbasis disk sehingga tidak dibatasi RAM |
| Data berukuran ratusan MB dan cukup diolah sekali | Skrip biasa di satu mesin atau Spark mode lokal | Biaya awal dan biaya kerangka kerja terdistribusi bisa lebih besar daripada manfaatnya |

---

## Daftar Pustaka

1. Dean, J., & Ghemawat, S. (2004). MapReduce: Simplified Data Processing on Large Clusters.
   *Proceedings of the 6th Symposium on Operating Systems Design and Implementation (OSDI '04)*.
2. Zaharia, M., Chowdhury, M., Das, T., Dave, A., Ma, J., McCauley, M., Franklin, M. J., Shenker, S.,
   & Stoica, I. (2012). Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory
   Cluster Computing. *Proceedings of the 9th USENIX Symposium on Networked Systems Design and
   Implementation (NSDI '12)*.
3. Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., Meng, X., Kaftan, T.,
   Franklin, M. J., Ghodsi, A., & Zaharia, M. (2015). Spark SQL: Relational Data Processing in Spark.
   *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data*.
4. Apache Software Foundation. *Apache Spark Documentation* (RDD Programming Guide; Spark SQL,
   DataFrames and Datasets Guide). https://spark.apache.org/docs/latest/
5. Yelp. *mrjob Documentation*. https://mrjob.readthedocs.io/
6. Internet Traffic Archive. *NASA-HTTP: Two months of HTTP requests to the NASA Kennedy Space
   Center WWW server*. https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

---

**Lampiran:** notebook `Tugas2_Kelompok21_25.55.2927_EkaPutraKurnia.ipynb` (kode lengkap dan
output eksekusi di Google Colab).
