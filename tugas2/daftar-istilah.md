# Daftar Istilah — Tugas 2 (Hadoop MapReduce vs Spark)

Penjelasan istilah yang muncul di [spesifikasi-tugas2.md](spesifikasi-tugas2.md), dengan bahasa
sehari-hari. Kolom **Di tugas** menunjukkan bagian spesifikasi tempat istilah itu muncul;
"(latar)" = tidak disebut di tugas, tapi perlu untuk memahaminya.

> **Konteks tugas ini**
>
> - **Posisi dalam arsitektur Big Data:** lapisan *processing*, jalur *batch*. Yang dibandingkan adalah
>   dua "mesin pengolah" (MapReduce vs Spark), bukan cara mengumpulkan, menyimpan, atau menyajikan data.
> - **Skala simulasi:** semuanya berjalan di **satu mesin Colab** dengan beberapa inti CPU, bukan
>   cluster banyak server. Tidak ada HDFS dan tidak ada biaya jaringan.
> - **Yang sebenarnya dibandingkan:** **mrjob (Python murni, runner lokal)** vs **Spark
>   (`local[*]`)**, bukan Hadoop Java di cluster vs Spark di cluster. Ini perlu disebut sebagai
>   keterbatasan di laporan.

**Daftar isi**

1. [Konsep umum](#1-konsep-umum)
2. [Hadoop & MapReduce](#2-hadoop--mapreduce)
3. [Apache Spark](#3-apache-spark)
4. [Benchmarking & sumber daya](#4-benchmarking--sumber-daya)
5. [Lingkungan kerja & alat](#5-lingkungan-kerja--alat)
6. [Data & studi kasus](#6-data--studi-kasus)
7. [Gambaran besar: alur kedua framework](#7-gambaran-besar-alur-kedua-framework)

---

## 1. Konsep umum

| Istilah | Penjelasan | Di tugas |
|---|---|---|
| **Big Data** | Dipakai dalam tiga arti: **(1) sifat data** yang ciri **3V**-nya membuat satu komputer biasa kewalahan: *Volume* (sangat besar), *Velocity* (cepat bertambah), *Variety* (bentuk beragam), sering ditambah *Veracity* (bisa dipercaya?) dan *Value* (bernilai?); **(2) arsitektur/infrastruktur** berlapis untuk menampung dan mengolah data seperti itu (lihat tabel lapisan di bawah); **(3) bidang ilmu/praktik** analisis data skala besar. Di tugas ini: arti (2) di **lapisan processing jalur batch**, dan dari arti (1) hanya **Volume** yang relevan (dataset 100 MB–1 GB). | §1 |
| **Batch processing** (pemrosesan batch) | Mengolah sekumpulan data sekaligus dalam satu kali jalan, bukan satu-satu saat data datang. Analogi: mencuci baju seminggu sekali sekaligus, bukan setiap baju kotor langsung dicuci. Lawannya: *stream processing* (real-time). | Judul, §1 |
| **Pipeline** | Rangkaian langkah pengolahan data yang berurutan: baca data → bersihkan/potong → hitung → simpan hasil. Seperti jalur produksi pabrik. | Judul, §1 |
| **Komputasi terdistribusi** | Pekerjaan dipecah dan dikerjakan bersamaan oleh banyak komputer, lalu hasilnya digabung. Analogi: 10 orang menghitung suara pemilu di 10 TPS, lalu hasilnya dijumlahkan. **Di Colab:** semuanya berjalan di satu mesin dengan banyak inti CPU. Itu sebenarnya **komputasi paralel** yang *mensimulasikan* komputasi terdistribusi; framework-nya sama, tapi tidak ada komunikasi antar-mesin lewat jaringan. | §1, §3 |
| **Framework** (kerangka kerja) | "Rangka" perangkat lunak siap pakai; kita tinggal mengisi bagian logikanya. Hadoop dan Spark adalah framework: mereka mengurus pembagian kerja, kita cukup menulis apa yang dihitung. | §1 |
| **Paradigma** | Cara berpikir/gaya dasar dalam menyelesaikan masalah. "Paradigma MapReduce" = semua pekerjaan dipaksa berbentuk tahap Map lalu Reduce. | §1 |
| **API** (*Application Programming Interface*) | Kumpulan perintah/fungsi yang disediakan sebuah library untuk dipakai programmer. "RDD API" dan "DataFrame API" adalah dua gaya perintah berbeda di Spark. | §1, Tahap 3 |
| **Dataset** | Kumpulan data yang diolah, biasanya berupa berkas (`.log`, `.csv`, dll.). | §3 |
| **Agregasi** | Meringkas banyak baris data menjadi angka ringkasan: jumlah, total, rata-rata, maksimum. Contoh: dari jutaan baris log → "IP X melakukan 523 request". | §3 |
| **Trade-off** | Untung-rugi; mendapat satu hal dengan mengorbankan hal lain. Contoh: Spark lebih cepat tapi lebih boros RAM. | §2 |

**Lapisan arsitektur Big Data** (posisi tugas ini ditandai ◄):

| Lapisan | Fungsi | Contoh teknologi | Di tugas ini |
|---|---|---|---|
| Sumber data | Asal data | log server, transaksi, sensor | Dataset NASA log / Online Retail |
| *Ingestion* | Memasukkan data ke sistem | Kafka, Flume, Sqoop | Unduh berkas ke Colab (`wget`) |
| *Storage* | Menyimpan secara terdistribusi | HDFS, Amazon S3, *data lake* | Disk lokal Colab (pengganti HDFS) |
| *Resource management* | Membagi CPU/RAM cluster | YARN, Kubernetes | Tidak ada (satu mesin) |
| ***Processing*** ◄ | Mengolah data: *batch* atau *stream* | **MapReduce, Spark** (batch); Flink, Spark Streaming (stream) | **Inti tugas: mrjob vs Spark RDD vs Spark DataFrame** |
| *Serving / analytics* | Menyajikan hasil | Hive, Presto, dashboard BI | Tabel & grafik di notebook/laporan |

Pola arsitektur terkenal: **Lambda** (jalur batch + jalur stream berdampingan) dan **Kappa** (hanya
jalur stream). Tugas ini hanya mengerjakan jalur batch.

## 2. Hadoop & MapReduce

| Istilah | Penjelasan | Di tugas |
|---|---|---|
| **Apache Hadoop** | Ekosistem open-source untuk menyimpan dan mengolah data besar di banyak komputer. Dua bagian intinya: HDFS (penyimpanan) dan MapReduce (pengolahan). | Judul, §1 |
| **HDFS** (*Hadoop Distributed File System*) | Sistem berkas Hadoop yang memecah berkas besar menjadi blok-blok dan menyebarkannya (beserta salinan cadangannya) ke banyak komputer. **Tidak dipakai di tugas ini:** data dibaca dari disk lokal Colab. Tetap perlu disebut di Bab 1 sebagai pembanding arsitektur sungguhan. | (latar) |
| **MapReduce** | Model pemrograman dengan dua tahap utama: **Map** (pecah & tandai data) dan **Reduce** (gabung & hitung). Di antaranya ada **Shuffle**. Hasil antar-tahap ditulis ke disk. | §1, §2, Tahap 2 |
| **Map / Mapper** | Fungsi yang membaca data baris per baris dan mengeluarkan pasangan *key-value*. Di contoh kode: dari setiap baris log, ambil IP lalu keluarkan `(ip, 1)`. | Tahap 2 |
| **Shuffle** (dan *sort*) | Tahap otomatis di antara Map dan Reduce: semua pasangan dengan **key yang sama** dikumpulkan ke tempat yang sama dan diurutkan. Analogi: semua surat dipilah ke kotak berdasarkan kode pos. Tahap ini mahal karena data dipindah-pindah (lewat disk/jaringan). | §2, Bab 3 |
| **Reduce / Reducer** | Fungsi yang menerima satu key beserta semua value-nya, lalu meringkasnya. Di contoh kode: `(ip, [1,1,1,…])` → `(ip, jumlah)`. | Tahap 2 |
| **Combiner** | "Reducer kecil" yang dijalankan di sisi Map **sebelum** shuffle. Misalnya satu mapper sudah menjumlahkan `(ip, 1)` miliknya sendiri menjadi `(ip, 37)`, sehingga data yang perlu di-shuffle jauh lebih sedikit. Di mrjob ditulis sebagai method `combiner`. **Contoh kode Tahap 2 tidak memakainya**, sedangkan `reduceByKey` di Spark melakukannya otomatis. Akibatnya perbandingan belum setara; layak diuji (dengan & tanpa combiner) dan dibahas di Bab 3. | (latar, Bab 3) |
| **Key-value pair** | Pasangan "kunci → nilai", misalnya `("192.168.1.1", 1)`. Bahasa dasar MapReduce dan juga RDD Spark. | Tahap 2–3 |
| **`yield`** | Kata kunci Python untuk "mengeluarkan" hasil satu per satu tanpa mengakhiri fungsi. mrjob memakai `yield` untuk mengirim pasangan key-value ke tahap berikutnya. | Tahap 2 |
| **Hadoop Streaming** | Fitur Hadoop agar Mapper/Reducer bisa ditulis dalam bahasa apa pun (termasuk Python), bukan hanya Java. Data dikirim lewat input/output standar (*stdin/stdout*). **Di Colab biasanya tidak terpakai:** mrjob baru memakai Hadoop Streaming bila dijalankan dengan runner `hadoop` di atas instalasi Hadoop. | §2 |
| **mrjob** | Library Python untuk menulis job MapReduce dengan mudah (cukup buat kelas berisi `mapper` dan `reducer`). Bisa dijalankan lokal tanpa Hadoop, atau dikirim ke cluster Hadoop sungguhan. | §2, Tahap 1–2 |
| **Runner** (mrjob) | Cara mrjob menjalankan job: `inline` (satu proses Python, untuk tes), `local` (meniru Hadoop dengan beberapa proses di satu komputer), `hadoop` (cluster sungguhan). Di Colab biasanya `inline`/`local`. *Tidak disebut di tugas, tapi menentukan cara kerja kode Tahap 2.* | (latar) |
| **Disk-based / intermediate I/O** | Ciri MapReduce: hasil antara (*intermediate*) setiap tahap ditulis ke disk lalu dibaca lagi oleh tahap berikutnya. Aman jika ada kegagalan, tapi lambat karena disk jauh lebih lambat daripada RAM. **Di tugas ini:** mrjob runner lokal menulis hasil antara sebagai berkas sementara di disk Colab, bukan di HDFS. | §1, Bab 3 |
| **Job** | Satu kali eksekusi pekerjaan MapReduce/Spark dari input sampai output. | Tahap 2 |

## 3. Apache Spark

| Istilah | Penjelasan | Di tugas |
|---|---|---|
| **Apache Spark** | Framework pengolahan data terdistribusi penerus gagasan MapReduce, dengan ciri utama menyimpan data antara di **memori (RAM)**, sehingga umumnya jauh lebih cepat. | Judul, §1 |
| **PySpark** | Antarmuka Python untuk Spark. Spark sendiri berjalan di Java (JVM); PySpark mengirim perintah Python ke sana lewat **Py4J**. | §2, Tahap 1 |
| **Py4J** | Jembatan yang memungkinkan Python memanggil objek Java. Notebook Python kita = "remote control"; pekerjaan beratnya dikerjakan JVM. | (latar) |
| **Driver / executor** | *Driver* = program pengatur yang menyusun DAG dan membagi task; *executor* = pekerja yang menjalankan task. Di cluster, executor tersebar di banyak mesin. | (latar) |
| **Local mode vs cluster mode** | *Cluster mode*: driver dan executor tersebar di banyak mesin, dikelola YARN/Kubernetes. *Local mode* (`local[*]`, dipakai di tugas ini): driver dan executor **berada dalam satu proses JVM** di satu mesin. Jadi RAM Spark terkumpul di proses JVM itu, bukan di proses Python notebook. | Tahap 3 |
| **SparkSession** | "Pintu masuk" utama program Spark. Dibuat sekali di awal; dari sini kita membaca data dan menjalankan perintah. | Tahap 3 |
| **SparkContext** (`spark.sparkContext`) | Pintu masuk versi lama, khusus untuk RDD API. Ada di dalam SparkSession. | Tahap 3 |
| **`appName`** | Nama aplikasi Spark, hanya label untuk identifikasi (muncul di log/Spark UI). | Tahap 3 |
| **`master("local[*]")`** | Menentukan di mana Spark dijalankan. `local[*]` = di komputer ini saja, memakai **semua inti CPU** yang ada. (`local[2]` = 2 inti.) Di Colab, ini cara "meniru" cluster. | Tahap 3 |
| **`getOrCreate()`** | Pakai SparkSession yang sudah ada, atau buat baru jika belum ada. | Tahap 3 |
| **RDD** (*Resilient Distributed Dataset*) | Struktur data dasar Spark: kumpulan data yang dipecah ke beberapa bagian (partisi) dan bisa diolah paralel. *Resilient* = jika ada bagian hilang, Spark bisa membangunnya ulang dari riwayat langkahnya. Gaya pemrogramannya mirip MapReduce (`map`, `reduceByKey`). | §1, §2, Tahap 3 |
| **DataFrame** | Data berbentuk tabel (baris & kolom bernama), mirip tabel SQL atau pandas. Lebih mudah ditulis dan di PySpark biasanya lebih cepat dari RDD karena dua hal: **(1)** fungsi bawaannya (`split`, `groupBy`, `count`) berjalan langsung di JVM tanpa bolak-balik ke Python (lihat *Python worker & serialisasi*); **(2)** dioptimalkan otomatis oleh *Catalyst* dan *Tungsten*. | §1, §2, Tahap 3 |
| **Python worker & serialisasi** | Saat RDD memakai fungsi Python (`lambda`), Spark harus menyalakan **proses Python tambahan** (*Python worker*) dan mengubah setiap data dari format Java ke Python lalu kembali lagi (*serialisasi/deserialisasi*). Proses bolak-balik ini mahal, dan inilah alasan utama **RDD + lambda di PySpark lebih lambat daripada DataFrame**. Poin penting untuk Bab 3. | (latar, Bab 3) |
| **Catalyst & Tungsten** | Dua mesin optimasi bawaan DataFrame/SQL. *Catalyst* menulis ulang rencana query agar efisien (misal membuang kolom yang tak perlu sedini mungkin); *Tungsten* menyimpan data dalam format biner ringkas dan menghasilkan kode yang cepat. RDD tidak mendapat optimasi ini. | (latar, Bab 3) |
| **Partisi** | Potongan dari dataset yang diolah oleh satu tugas (*task*) secara paralel. Jumlah partisi memengaruhi seberapa paralel pekerjaan berjalan. | (latar) |
| **Transformation** | Perintah yang **mendefinisikan** pengolahan, tapi belum menjalankannya: `map`, `filter`, `reduceByKey`, `select`, `groupBy`, `agg`. | Tahap 3 |
| **Action** | Perintah yang **memicu** pengolahan benar-benar dijalankan dan mengembalikan/menyimpan hasil: `collect()`, `count()`, `show()`, `take()`, `write…`. **Penting:** contoh kode Tahap 3 belum memuat action, jadi belum ada yang dihitung. | (latar, penting untuk Tahap 4) |
| **Lazy evaluation** | Spark "menunda" pekerjaan: transformation hanya dicatat sebagai rencana, baru dikerjakan ketika ada action. Analogi: menulis daftar belanja dulu, baru pergi ke pasar sekali jalan. Keuntungannya: Spark bisa menyusun rencana paling efisien. | §2 |
| **DAG** (*Directed Acyclic Graph*) | "Peta rencana kerja" Spark: diagram langkah-langkah (node) yang dihubungkan panah searah tanpa putaran balik. Dibuat dari rangkaian transformation, lalu dipecah menjadi *stage* dan *task*. Bandingkan dengan MapReduce yang hanya punya pola tetap Map→Reduce. | §2, Bab 3 |
| **Narrow vs wide transformation** | *Narrow*: tiap partisi bisa diolah sendiri tanpa data dari partisi lain (`map`, `filter`, `select`). *Wide*: butuh data dari banyak partisi, sehingga perlu **shuffle** (`reduceByKey`, `groupBy`). Transformasi wide inilah yang menjadi batas antar-stage di DAG. | (latar, Bab 3) |
| **Stage / Task** | Bagian dari DAG. Satu *stage* = sekelompok transformasi narrow yang dijalankan berurutan tanpa shuffle; batas antar-stage adalah transformasi wide (shuffle). Satu *task* = pekerjaan satu stage pada satu partisi. | (latar, Bab 3) |
| **Pipelining** | Di dalam satu stage, Spark menjalankan beberapa transformasi narrow sekaligus per baris (baca → `split` → ambil IP) **tanpa menyimpan hasil antaranya**. MapReduce tidak bisa begini antar-job: setiap job menulis output ke disk. | (latar, Bab 3) |
| **In-memory processing** | Spark menyimpan data antara di RAM sebisa mungkin, bukan ditulis ke disk tiap tahap seperti MapReduce. **Batasannya:** saat shuffle, Spark *tetap menulis* data ke disk lokal. Untuk job sekali jalan seperti di tugas ini (hitung request per IP), keunggulan Spark lebih banyak berasal dari *pipelining*, *map-side combine* pada `reduceByKey`, dan biaya task yang kecil. Keunggulan "in-memory" baru sangat terasa bila data yang sama **dipakai berulang** (lihat *caching*). | §1, Bab 3 |
| **Caching** (`cache()` / `persist()`) | Menyuruh Spark menyimpan hasil suatu langkah di memori agar tidak dibaca dan dihitung ulang jika dipakai lagi. **Di tugas ini** relevan bila beberapa analisis (misal per IP, per status code, per jam) dijalankan pada dataset yang sama. MapReduce harus membaca ulang berkas dari disk untuk setiap job. | Rubrik |
| **Spill** | Data "ditumpahkan" ke disk karena memori tidak cukup menampungnya. Terjadi di MapReduce maupun Spark; bila sering terjadi, Spark kehilangan keunggulan in-memory-nya. | (latar, Bab 3) |
| **`textFile()` / `spark.read.text()`** | Membaca berkas teks: versi RDD menghasilkan satu elemen per baris; versi DataFrame menghasilkan tabel dengan satu kolom bernama `value`. | Tahap 3 |
| **`map(lambda …)`** | Menerapkan fungsi ke setiap elemen. `lambda` = fungsi kecil tanpa nama di Python. | Tahap 3 |
| **`reduceByKey`** | Menggabungkan semua value dengan key yang sama memakai fungsi tertentu (di contoh: dijumlahkan). Setara **Combiner + Shuffle + Reduce**: Spark otomatis menjumlahkan dulu di tiap partisi (*map-side combine*) sebelum shuffle. | Tahap 3 |
| **`split`, `col`, `getItem`, `alias`** | Fungsi DataFrame: `split` memecah teks, `col` merujuk kolom, `getItem(0)` mengambil potongan pertama, `alias` memberi nama kolom baru. | Tahap 3 |
| **`groupBy` + `agg(count(...))`** | Mengelompokkan baris berdasarkan kolom (misal `ip`), lalu menghitung ringkasan per kelompok (di contoh: jumlah baris). Setara `GROUP BY` di SQL. | Tahap 3 |

## 4. Benchmarking & sumber daya

| Istilah | Penjelasan | Di tugas |
|---|---|---|
| **Benchmarking** | Mengukur dan membandingkan performa secara adil dan terukur. "Valid" berarti: input sama, lingkungan sama, diulang beberapa kali, dan yang diukur memang hal yang ingin dibandingkan. | §2, Tahap 4 |
| **Execution time** (waktu eksekusi) | Lama waktu dari mulai sampai selesai, dalam detik. Di contoh kode diukur dengan `time.time()` sebelum dan sesudah. | §1, Bab 2 |
| **Resource usage** (konsumsi sumber daya) | Seberapa banyak RAM dan CPU yang dipakai saat program berjalan. | §1, Bab 2 |
| **RAM** | Memori kerja komputer: cepat tapi terbatas dan hilang saat mati. Di Colab gratis sekitar ±12 GB. | §1, Bab 2 |
| **CPU %** | Persentase pemakaian prosesor. Di mesin multi-inti bisa lebih dari 100% (misal 200% = dua inti penuh). Diminta di Bab 2, tapi belum ada di contoh kode. | Bab 2 |
| **Disk I/O** | Kegiatan baca/tulis ke penyimpanan (disk). Jauh lebih lambat daripada akses RAM. | §1, Bab 3 |
| **RSS** (*Resident Set Size*) | Banyaknya RAM fisik yang sedang dipakai sebuah proses. `memory_info().rss` mengembalikan nilai ini dalam byte; dibagi `1024*1024` agar menjadi MB. | Tahap 4 |
| **Memory usage delta** | Selisih RAM sesudah dikurangi sebelum menjalankan fungsi. Kelemahan: hanya mengukur proses yang diukur (lihat *proses* di bawah) dan tidak menangkap puncak (*peak*) pemakaian di tengah jalan. | Tahap 4 |
| **Proses / PID / child process** | Proses = program yang sedang berjalan; PID = nomor identitasnya; `os.getpid()` = PID notebook itu sendiri. *Child process* = proses lain yang dijalankan olehnya. Spark (JVM) berjalan sebagai proses lain, jadi RAM-nya **tidak** terhitung jika hanya mengukur `os.getpid()`. Untuk mrjob, tergantung cara menjalankannya: lewat `!python mapreduce_job.py …` = proses terpisah (tidak terukur); lewat `make_runner()` di dalam notebook dengan runner `inline` = proses yang sama (terukur); runner `local` = memakai subproses. | Tahap 4 |
| **JVM** (*Java Virtual Machine*) | Mesin tempat program Java berjalan. Hadoop dan Spark ditulis dengan Java/Scala, sehingga butuh JVM. Itu sebabnya Tahap 1 menginstal Java JDK. | Tahap 1 |
| **Java JDK** (`openjdk-8-jdk-headless`) | Paket Java untuk menjalankan/mengembangkan program Java. *Headless* = versi tanpa komponen tampilan grafis (cocok untuk server/Colab). | Tahap 1 |
| **Overhead** | Biaya "tambahan" di luar pekerjaan inti, misal waktu menyalakan JVM, membuat proses, menulis berkas sementara. *I/O overhead* = biaya tambahan akibat baca/tulis disk. | Rubrik |
| **Biaya shuffle** | Waktu dan sumber daya yang habis untuk memindahkan dan mengurutkan data antar-tahap. Sering menjadi bagian paling mahal dari job, baik di MapReduce maupun Spark. | Bab 3 |
| **Garbage collection (GC)** | Proses otomatis membersihkan memori yang sudah tidak dipakai (di JVM maupun Python). Saat GC berjalan, program bisa "tersendat", sehingga memengaruhi waktu dan angka RAM yang terukur. | Rubrik |
| **Warm-up / cold start** | Eksekusi pertama biasanya lebih lambat (JVM baru menyala, berkas belum ada di cache sistem). Benchmark yang baik memisahkan atau mengabaikan putaran pertama dan mengambil rata-rata beberapa kali. | (latar, Tahap 4) |

## 5. Lingkungan kerja & alat

| Istilah | Penjelasan | Di tugas |
|---|---|---|
| **Google Colaboratory (Colab)** | Layanan Google untuk menjalankan notebook Python di browser, memakai komputer milik Google (gratis, dengan batasan). Komputernya bersifat sementara: berkas hilang saat runtime di-reset. | §1, §4 |
| **Runtime** (Colab) | "Mesin virtual" yang menjalankan notebook di Colab. *Restart runtime* = memulai ulang Python; *disconnect/delete runtime* = berkas yang diunggah ikut hilang. | (latar) |
| **Jupyter Notebook (`.ipynb`)** | Format dokumen interaktif berisi **sel kode** dan **sel teks (Markdown)** beserta outputnya. Colab memakai format ini. | §5 |
| **Sel (cell)** | Satu kotak di notebook; bisa berisi kode atau teks. | §4 |
| **`!` di awal baris** | Menjalankan perintah terminal (shell) dari dalam notebook, misal `!pip install …`. | Tahap 1 |
| **`%%writefile nama.py`** | *Magic command* Jupyter: isi sel tidak dijalankan, melainkan disimpan menjadi berkas `nama.py`. Dipakai karena mrjob perlu dijalankan sebagai skrip terpisah. | Tahap 2 |
| **`apt-get install`** | Perintah Linux untuk memasang program sistem (di sini: Java). `-qq` = sangat senyap; `> /dev/null` = buang outputnya. | Tahap 1 |
| **`pip install -q`** | Memasang library Python. `-q` = *quiet* (output sedikit). | Tahap 1 |
| **psutil** | Library Python untuk membaca info sistem: RAM, CPU, proses yang berjalan. | Tahap 1, 4 |
| **`time`** | Modul bawaan Python untuk mengukur waktu. (Untuk mengukur durasi, `time.perf_counter()` lebih presisi daripada `time.time()`.) | Tahap 4 |
| **Restart & Run All** | Menjalankan ulang seluruh notebook dari nol, berurutan dari sel pertama. Syarat tugas: harus lolos tanpa error, artinya semua langkah (termasuk unduh dataset) harus ada di notebook. | §5 |
| **PEP8** | Panduan gaya penulisan kode Python resmi: indentasi 4 spasi, nama variabel `huruf_kecil_garis_bawah`, nama kelas `HurufBesarDiAwal`, panjang baris wajar, dll. | Rubrik |
| **Markdown** | Format teks sederhana untuk judul, daftar, tebal/miring (seperti berkas ini). Dipakai di sel teks notebook untuk menjelaskan kode. | Rubrik |
| **Visualisasi benchmarking** | Grafik (misal batang) yang membandingkan waktu/RAM/CPU ketiga pendekatan. Biasanya dibuat dengan matplotlib. | §5 |

## 6. Data & studi kasus

| Istilah | Penjelasan | Di tugas |
|---|---|---|
| **Log web server** | Catatan otomatis server web untuk setiap permintaan yang masuk: siapa (IP), kapan, halaman apa, hasilnya (status code), berapa besar datanya. Satu baris = satu request. | §3 |
| **NASA HTTP Access Logs** | Dataset log publik dari server web NASA tahun 1995, sering dipakai untuk latihan pemrosesan log. | §3 |
| **Kaggle** | Situs komunitas data science yang menyediakan banyak dataset gratis. | §3 |
| **Request** | Satu permintaan dari browser/klien ke server, misal membuka satu halaman atau satu gambar. | §3 |
| **IP Address** | Alamat numerik perangkat di jaringan, misal `192.168.1.10`. Pada log umumnya kolom pertama, makanya contoh kode memakai `split(" ")[0]`. | §3, Tahap 2–3 |
| **Endpoint** | Alamat halaman/sumber yang diminta, misal `/images/logo.gif` atau `/api/login`. | §3 |
| **HTTP Status Code** | Kode hasil request: **200** = berhasil, **404** = tidak ditemukan, **500** = server error (kode 3xx = dialihkan, 4xx = kesalahan klien, 5xx = kesalahan server). | §3 |
| **Bytes transferred / traffic** | Besar data yang dikirim server untuk setiap request. Dijumlahkan per jam = total traffic per jam. | §3 |
| **Clickstream** | Jejak urutan klik pengguna saat menjelajah sebuah situs. | §3 |
| **Online Retail II** | Dataset transaksi toko online asal Inggris (2009–2011): nomor faktur, produk, jumlah, harga, pelanggan, negara. | §3 |
| **Kategori produk / negara** | Kolom pengelompokan pada data transaksi untuk menghitung total penjualan per kelompok. | §3 |
| **Frekuensi transaksi bulanan** | Berapa kali seorang pelanggan bertransaksi dalam setiap bulan. | §3 |
| **Anomali (transaksi ekstrem)** | Data yang jauh berbeda dari kebanyakan, misal transaksi bernilai sangat besar. Biasanya dicari dengan batas statistik (misal di atas persentil 99 atau di luar rata-rata ± 3 simpangan baku). | §3 |

## 7. Gambaran besar: alur kedua framework

```
MapReduce (mrjob, runner lokal di Colab):
  berkas input ─► MAP ─► [berkas sementara di DISK] ─► SHUFFLE & SORT ─► REDUCE ─► berkas output
                 (ip,1) untuk SETIAP baris            kumpulkan per ip   (ip, total)
                 (tanpa combiner: semua (ip,1) ikut di-shuffle)

Spark (RDD / DataFrame, local[*] = satu JVM):
  berkas input ─► transformation … transformation  (hanya dicatat → DAG, lazy)
                                   │
                         ACTION (count/collect/write)
                                   │
                 Stage 1 (narrow, di-pipeline: baca → split → ambil ip → jumlah sementara per partisi)
                                   │
                         SHUFFLE (tetap ditulis ke disk lokal, tapi datanya sudah kecil)
                                   │
                 Stage 2 (jumlahkan per ip) ─► hasil
```

**Inti perbandingan untuk Bab 3**

| Aspek | MapReduce (mrjob di tugas ini) | Apache Spark |
|---|---|---|
| Bahasa eksekusi | Python murni | JVM; RDD + `lambda` juga memakai Python worker |
| Data antara di dalam job | Ditulis ke disk antar-tahap | Langkah narrow di-pipeline di memori; shuffle tetap ke disk lokal |
| Data antara antar-job | Selalu ditulis/dibaca ulang dari disk | Bisa di-`cache()` di RAM untuk dipakai ulang |
| Pengurangan data sebelum shuffle | Hanya bila ditulis `combiner` (contoh kode tidak memakainya) | Otomatis pada `reduceByKey` / `groupBy().agg()` |
| Bentuk alur kerja | Kaku: Map → Shuffle → Reduce (pekerjaan rumit = beberapa job berantai) | Fleksibel: DAG banyak langkah, dioptimalkan sebelum dijalankan |
| Kapan dijalankan | Langsung saat job dijalankan | Lazy: menunggu action |
| Overhead awal | Kecil (hanya Python) | Besar (menyalakan JVM + SparkSession); bisa dominan di data kecil |
| Kecepatan | Cenderung lebih lambat | Umumnya lebih cepat, tapi **belum tentu** di data 100 MB–1 GB jika overhead awal ikut diukur |
| Kebutuhan RAM | Cenderung lebih hemat | Cenderung lebih boros (JVM + cache) |
| Kemudahan kode | Harus dipikirkan dalam bentuk mapper/reducer | RDD mirip MapReduce; DataFrame mirip SQL, lebih ringkas |

> Angka di laporan harus berasal dari hasil pengukuran Anda sendiri; kolom "cenderung/umumnya"
> di atas adalah dugaan awal (hipotesis) yang perlu dibuktikan atau dibantah oleh benchmark.
