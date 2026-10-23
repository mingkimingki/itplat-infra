# LEMBAR KERJA DISKUSI: STRATEGI BIG DATA SMART CITY

**Mata Kuliah:** IT Platform and Infrastructure
**Kelompok:** [Isi Nama Kelompok]
**Anggota:** [Isi Nama Anggota]

> Status: DRAFT. Edit di sini dulu, lalu dimasukkan kembali ke file .docx setelah final.
> Angka (5.000 kamera, 1.000 sensor, 2–4 Mbps per kamera) adalah **asumsi** untuk menggambarkan skala, bukan data resmi.

---

## BAGIAN 1: Pemetaan Karakteristik Big Data (The 5 V's)

### 1. Kamera CCTV (Video 24/7)
**Aspek utama:** Volume (utama), Velocity, Variety

- **Volume:** Satu kamera 1080p menghasilkan ±2–4 Mbps atau ±20–40 GB video per hari. Dengan asumsi 5.000 kamera, kota menghasilkan ±100–200 TB per hari, ratusan TB hingga petabyte per bulan. Ini sumber data terbesar dan penyebab langsung server tradisional overload.
- **Velocity:** Video mengalir terus-menerus 24/7. Informasi kepadatan lalu lintas (jumlah kendaraan, panjang antrean, dan kecepatan arus di tiap persimpangan) harus diekstrak dalam hitungan detik. Kalau terlambat, datanya sudah tidak menggambarkan kondisi jalan saat itu dan tidak berguna untuk mengatur lampu lalu lintas.
- **Variety:** Data tidak terstruktur (video). Harus diubah dulu menjadi data terstruktur dengan computer vision (misalnya model deteksi objek seperti YOLO):
  - **Jumlah kendaraan dan panjang antrean** per lajur.
  - **Klasifikasi jenis kendaraan:** motor, mobil, bus, truk/kendaraan berat. Klasifikasi ini penting karena:
    - *Untuk lalu lintas:* setiap jenis kendaraan punya bobot berbeda dalam satuan mobil penumpang (smp). Satu truk memakan ruang jalan jauh lebih besar daripada satu motor, sehingga durasi lampu hijau dihitung dari komposisi kendaraan, bukan sekadar jumlahnya.
    - *Untuk polusi:* kendaraan berat bermesin diesel menyumbang PM2.5 jauh lebih besar per unit. Komposisi kendaraan dari CCTV bisa dikorelasikan dengan data sensor AQI untuk mengetahui jenis kendaraan mana yang paling berkontribusi terhadap polusi di suatu ruas.
- *Implikasi infrastruktur:* **Edge computing**. Di setiap node CCTV dipasang komputer mini dengan akselerator AI (misalnya NVIDIA Jetson, atau Raspberry Pi + modul Coral/Hailo). Perangkat ini langsung menjalankan model computer vision di lokasi: mendeteksi, menghitung, dan mengklasifikasikan kendaraan. Yang dikirim ke pusat hanya **hasil analisisnya** dalam bentuk data terstruktur (metadata), bukan video mentah. Contoh output setiap beberapa detik:
  ```json
  {"cctv_id": "SDR-012", "waktu": "2026-10-03T07:15:00", "lajur": 2,
   "motor": 34, "mobil": 12, "bus": 1, "truk": 3,
   "panjang_antrean_m": 85, "kecepatan_rata2_kmh": 14}
  ```
  - **Dampaknya:** Data yang dikirim turun dari megabit per detik (video) menjadi beberapa kilobyte (teks JSON). Masalah bandwidth dan overload server pusat teratasi, dan respons lebih cepat karena analisis tidak perlu menunggu video sampai ke pusat.
  - **Video tetap disimpan secara selektif:** video mentah disimpan sementara di perangkat edge (buffer beberapa jam/hari). Hanya klip penting, misalnya saat terdeteksi kecelakaan atau pelanggaran, yang diunggah ke pusat. Arsipnya disimpan di HDFS/object storage.
  - Stream metadata dari ribuan node dikumpulkan dengan Kafka dan diproses real-time dengan Storm/Spark Streaming.

### 2. Sensor IoT (Kualitas Udara)
**Aspek utama:** Velocity (utama)

- **Velocity:** Setiap sensor mengirim data setiap detik. Dalam satu hari ada 86.400 detik (24 jam × 60 menit × 60 detik), jadi 1 sensor menghasilkan 86.400 record per hari. Dengan asumsi 1.000 sensor: 1.000 × 86.400 = ±86 juta record per hari. Data ini cepat "basi": nilai AQI 30 menit lalu kurang berguna untuk peringatan dini, sehingga harus diproses sebagai stream, bukan batch.
- **Catatan:** Ukuran per record kecil (angka PM2.5, AQI, timestamp, lokasi) dan terstruktur (time-series). Jadi Volume dan Variety-nya relatif rendah dibanding CCTV.
- *Implikasi infrastruktur:* Edge computing memfilter sinyal dan langsung memicu alert ketika kualitas udara melampaui ambang batas aman. Data dikumpulkan lewat Kafka + Spark Streaming/Flink ke time-series database.

### 3. GPS Ride-Hailing & Transportasi
**Aspek utama:** Variety (utama), Velocity

- **Variety:** Data spasio-temporal (lintang, bujur, waktu, kecepatan, arah) dari banyak penyedia: google, taxi online, bus, KRL. Masing-masing punya format, frekuensi pengiriman, dan skema ID berbeda, sehingga perlu diharmonisasi dan dicocokkan ke peta jalan (map-matching).
- **Velocity:** Ribuan kendaraan mengirim posisi secara terus-menerus. Inilah sumber terbaik untuk kecepatan riil per ruas jalan.
- *Implikasi infrastruktur:* Hadoop/Spark menghitung pola pergerakan per moda transportasi (matriks asal–tujuan, jam sibuk, ruas rawan macet). Spark Streaming dipakai untuk kecepatan real-time.

### 4. Media Sosial (Cuitan/Keluhan)
**Aspek utama:** Variety dan Veracity (masalah utama)

- **Variety:** Teks tidak terstruktur dengan bahasa informal: slang, singkatan, campur bahasa daerah/Inggris, emoji, foto, dan video. Butuh NLP (misalnya IndoBERT) untuk klasifikasi topik, sentimen, dan ekstraksi lokasi.
- **Veracity:** Kebenarannya paling diragukan: ada bot/buzzer, hoaks, hiperbola, sarkasme, lokasi tidak akurat, dan bias demografis pengguna (dibahas di Bagian 2).
- **Catatan:** Volume-nya relatif kecil dibanding CCTV, tetapi data ini satu-satunya sumber yang menangkap persepsi dan dampak langsung yang dirasakan warga.

---

## BAGIAN 2: Analisis Veracity (Integritas Data)

**Sumber Data Terpilih:** Media Sosial (cuitan/keluhan warga)

### Alasan Mengapa Rendah
Media sosial adalah satu-satunya sumber yang dibuat oleh manusia secara sukarela, tanpa standar pengukuran. Masalah veracity-nya berlapis:

1. **Bot & buzzer:** Akun otomatis atau kampanye terkoordinasi bisa membanjiri isu tertentu, misalnya menyerang kebijakan ganjil-genap atau menggiring opini politik, sehingga "volume keluhan" tidak mencerminkan kondisi nyata.
2. **Hoaks & konten daur ulang:** Foto banjir/macet lama diunggah ulang seolah-olah terjadi hari ini.
3. **Ambiguitas bahasa:** Sarkasme ("mantap, 2 jam di Sudirman"), hiperbola ("macet parah banget"), slang, dan campur kode membuat sentimen mudah salah dibaca mesin.
4. **Bias sampel:** Yang aktif di media sosial cenderung warga muda dan urban. Lansia dan warga berpenghasilan rendah, yang justru paling rentan terhadap polusi, kurang terwakili. Wilayah yang "sepi keluhan" belum tentu tidak bermasalah.
5. **Lokasi & waktu tidak presisi:** Sangat sedikit postingan yang punya geotag. Warga juga sering mengeluh setelah kejadian lewat, sehingga timestamp tidak sama dengan waktu kejadian.

### Strategi Mitigasi (Pembersihan Data)
Prinsip kami: media sosial diperlakukan sebagai **sinyal pendukung yang harus dikonfirmasi**, bukan dasar tunggal pengambilan kebijakan. Pipeline pembersihannya:

1. **Filter bot:** Skor kecurigaan per akun berdasarkan umur akun, rasio follower/following, frekuensi posting, dan pola jam posting yang tidak manusiawi. Akun dengan skor tinggi dikeluarkan atau diberi bobot rendah.
2. **Deduplikasi & deteksi kampanye:** Retweet dan teks copy-paste dihapus. Klaster teks yang sangat mirip dan muncul serentak dari banyak akun ditandai sebagai kampanye terkoordinasi.
3. **Normalisasi & NLP:** Kamus slang bahasa Indonesia, model IndoBERT untuk relevansi topik (lalu lintas/pernapasan) dan sentimen, plus geoparsing nama jalan/kelurahan menjadi koordinat. Akurasi model diuji dengan sampel yang dilabeli manual oleh manusia.
4. **Triangulasi dengan sumber objektif:** Keluhan "macet di Jl. X" baru dihitung valid jika data GPS menunjukkan kecepatan di ruas X memang turun dalam jendela ±30 menit. Keluhan "sesak napas" di Kecamatan Y dicocokkan dengan sensor AQI terdekat.
5. **Skor kepercayaan & koreksi bias:** Setiap postingan diberi confidence score. Hasil agregat diberi pembobotan (post-stratification) menurut demografi wilayah, dan dilengkapi kanal aduan resmi (aplikasi pengaduan kota, call center) agar kelompok yang tidak aktif di media sosial tetap terwakili.
6. **Catatan kritis:** Sensor IoT pun bisa bermasalah (kalibrasi bergeser/drift, sensor kotor, mati listrik). Bedanya, error sensor lebih mudah dideteksi secara statistik, misalnya dengan membandingkan sensor dengan tetangganya atau dengan stasiun referensi resmi.

---

## BAGIAN 3: Perumusan Value (Solusi Strategis)

### 1. Intervensi Lalu Lintas (Real-Time)
**Skenario:** Menggabungkan data GPS dan CCTV.

**Aksi:** *Adaptive Traffic Signal Control* (lampu lalu lintas adaptif) berbasis prediksi. CCTV (via edge computing) menghitung panjang antrean per lajur di persimpangan, sedangkan GPS memberi kecepatan riil dan arus kendaraan yang akan datang dari hulu 5–15 menit ke depan.

1. **Durasi lampu otomatis:** Durasi hijau/merah disesuaikan setiap siklus menurut panjang antrean aktual dan prediksi arus dari GPS, bukan jadwal tetap.
2. **Green wave koridor:** Lampu di sepanjang koridor utama dikoordinasikan agar kendaraan yang melaju di kecepatan tertentu mendapat hijau berturut-turut.
3. **Deteksi insiden otomatis:** Jika CCTV mendeteksi kendaraan berhenti dan kecepatan GPS di ruas itu anjlok, sistem otomatis mengirim petugas/derek dan mendorong rute alternatif ke aplikasi navigasi dan ride-hailing.
4. **Prioritas transportasi umum:** Bus yang terlambat (diketahui dari GPS) diberi perpanjangan lampu hijau agar angkutan umum lebih andal dan menarik dibanding kendaraan pribadi.
5. **Indikator keberhasilan (KPI):** Waktu tempuh rata-rata per koridor, panjang antrean di persimpangan, dan waktu tanggap insiden, dibandingkan sebelum dan sesudah intervensi.

### 2. Intervensi Kesehatan Publik/Polusi
**Skenario:** Menggabungkan data Sensor IoT dan Media Sosial.

**Aksi:** Sistem Peringatan Dini & Respons Kesehatan berbasis wilayah. Sensor IoT memberi ukuran objektif (PM2.5/AQI), sedangkan media sosial menangkap dampak yang dirasakan warga (keluhan sesak napas, batuk). Jika keduanya melonjak bersamaan di satu kecamatan, sinyalnya kuat dan valid untuk ditindaklanjuti.

1. **Peringatan per kelurahan:** Notifikasi push/SMS ke warga di zona terdampak, berisi anjuran memakai masker dan mengurangi aktivitas luar ruang. Sekolah setempat diminta memindahkan olahraga ke dalam ruangan.
2. **Siaga layanan kesehatan:** Puskesmas dan RS di zona tersebut disiagakan (stok obat ISPA, inhaler, tenaga medis) sebelum lonjakan pasien terjadi.
3. **Intervensi sumber polusi:** Jika polusi berkorelasi dengan kemacetan (dari GPS), berlakukan pembatasan kendaraan sementara (misalnya ganjil-genap dinamis atau zona rendah emisi) di zona itu. Jika klasifikasi kendaraan dari CCTV menunjukkan porsi truk/kendaraan berat diesel yang tinggi, pembatasan bisa lebih tepat sasaran, misalnya larangan truk melintas pada jam tertentu dan pengalihan ke jalan tol/lingkar luar. Jika polusi tinggi tanpa kemacetan, kirim tim inspeksi ke industri atau lokasi pembakaran sampah terdekat.
4. **Kebijakan jangka panjang:** Peta hotspot polusi historis dipakai untuk menentukan lokasi Low Emission Zone, penambahan ruang terbuka hijau, dan rute bus listrik.

---

## BAGIAN 4: Rekomendasi Arsitektur Teknologi

### 1. Skalabilitas (Scalability)
Jika sensor dan kamera bertambah 10 kali lipat, volume dan kecepatan data juga naik sekitar 10 kali lipat.

1. **Vertical scaling:** Mainframe punya batas fisik (jumlah CPU, RAM, slot disk). Begitu batas itu tercapai, satu-satunya jalan adalah membeli mesin yang lebih besar lagi dengan harga yang naik tidak linear, ditambah migrasi data yang berisiko dan butuh downtime.
2. **Horizontal scaling:** Cukup menambah node baru ke cluster. HDFS, Kafka, dan Spark mendistribusikan ulang data dan beban kerja secara otomatis, sehingga kapasitas tumbuh hampir linear sesuai kebutuhan tanpa mematikan sistem.
3. **Elastisitas:** Di cloud, cluster bisa diperbesar otomatis saat jam sibuk atau kejadian khusus (mudik, banjir) lalu diperkecil kembali saat sepi. Mainframe harus dibeli untuk kapasitas puncak walaupun sebagian besar waktunya menganggur.

### 2. Ketahanan (Fault Tolerance)
Satu mainframe adalah *single point of failure*.

1. **Jika mainframe mati:** Seluruh layanan berhenti bersamaan: lampu lalu lintas adaptif, peringatan polusi, dan dashboard. Untuk sistem kritis kota, ini tidak dapat diterima. Mainframe memang memiliki komponen redundan di dalamnya, tetapi tetap berada di satu lokasi fisik yang rentan terhadap banjir, kebakaran, atau gangguan listrik. Membeli mainframe cadangan berarti biayanya hampir dua kali lipat.
2. **Sistem terdistribusi:** Data direplikasi ke beberapa node (HDFS default 3 salinan, replikasi partisi di Kafka). Jika satu node mati, tugasnya otomatis dijalankan ulang di node lain (misalnya lewat lineage di Spark). Dampaknya hanya penurunan kinerja kecil, bukan sistem berhenti total (*graceful degradation*).
3. **Ketahanan geografis:** Node bisa tersebar di beberapa data center dan di edge. Jika koneksi ke pusat terputus, perangkat edge di persimpangan tetap bisa mengatur lampu secara lokal.

### 3. Efisiensi Biaya (Cost-Efficiency)
*Total Cost of Ownership* sistem terdistribusi umumnya jauh lebih rendah dan risikonya lebih kecil.

1. **Mainframe:** Investasi modal (CAPEX) puluhan miliar di depan, sebelum manfaatnya terbukti. Biaya ini ditambah lisensi dan kontrak pemeliharaan vendor, teknisi spesialis yang langka, dan ketergantungan pada satu vendor (*vendor lock-in*). Nilainya pun terdepresiasi dan kapasitasnya bisa usang dalam 5–7 tahun.
2. **Komputer komoditas/cloud:** Bisa dimulai kecil (pilot di beberapa koridor), lalu dibayar sesuai pemakaian (OPEX) dan diperbesar hanya bila terbukti bermanfaat. Software intinya (Hadoop, Spark, Kafka) open-source tanpa biaya lisensi. Hardware komoditas murah dan mudah diganti saat rusak.
3. **Rekomendasi:** Arsitektur hybrid: data sensitif seperti video CCTV yang memuat wajah dan pelat nomor diproses di edge/cluster on-premise, sementara analitik agregat berjalan di cloud region Indonesia, sesuai UU Pelindungan Data Pribadi (UU No. 27/2022). Sebagian anggaran yang dihemat dialokasikan untuk melatih SDM data engineer, karena sistem terdistribusi menuntut keahlian tersebut.

---

## KESIMPULAN KELOMPOK

Big Data mengubah tata kelola kota dari **reaktif** menjadi **proaktif**. Sebelumnya, pemerintah baru bertindak setelah macet atau krisis polusi terjadi dan dikeluhkan warga. Kini kebijakan bisa diambil berdasarkan bukti real-time dan prediksi, sehingga intervensi dilakukan sebelum masalah memuncak. Nilai terbesarnya bukan pada data dari satu sumber, melainkan pada **penggabungan sumber**: CCTV dan GPS untuk lalu lintas, sensor IoT dan media sosial untuk kesehatan, dengan data objektif dipakai untuk memvalidasi data subjektif. Agar itu tercapai, kota membutuhkan fondasi infrastruktur terdistribusi yang skalabel, tahan gangguan, dan hemat biaya, serta tata kelola kualitas data dan privasi yang ketat. Pada akhirnya, kota beralih dari kebijakan berbasis asumsi menuju kebijakan berbasis data yang terukur dan dapat dievaluasi secara terus-menerus.
