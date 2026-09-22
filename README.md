# Karhutla Data Pipeline

## Tujuan

Menyediakan data karhutla terstruktur hingga tingkat kabupaten/kota untuk kebutuhan analisis: cuaca, hotspot, kualitas udara, dan forecast.

## Sumber Data

| Data | Sumber | Kegunaan |
| --- | --- | --- |
| Master wilayah dan centroid | Emsifa | Provinsi, kabupaten/kota, koordinat wilayah |
| Weather historical | Visual Crossing | Cuaca harian historis |
| Weather forecast | Open-Meteo | Prediksi cuaca 3 hari ke depan |
| Air quality | Open-Meteo | AQI, PM2.5, PM10, CO, NO₂, dust, AOD, UV |
| Hotspot | NASA FIRMS | Titik panas, FRP, confidence, waktu, koordinat |

## Flow ETL

```text
Airflow
  ↓
Pilih provinsi Kalimantan: 61–65
  ↓
Ambil kabupaten/kota dan centroid dari Emsifa
  ↓
Extract: Visual Crossing + Open-Meteo + NASA FIRMS
  ↓
Transform: validasi nilai, normalisasi waktu, konversi satuan,
deduplikasi hotspot, dan mapping ke kabupaten/kota
  ↓
Load / upsert ke PostgreSQL atau MySQL
  ↓
Catat status run ke tb_r_etl_run
```

## Otomatisasi Airflow

- PostgreSQL: setiap hari pukul **09:00 WIB**.
- MySQL: setiap hari pukul **10:30 WIB**.
- Cakupan: provinsi Kalimantan kode `61`, `62`, `63`, `64`, dan `65`.
- Proses per provinsi berjalan berurutan.
- Jika gagal, Airflow retry maksimal **2 kali** dengan jeda **15 menit**.
- Status dan jumlah data setiap task disimpan di `tb_r_etl_run`.

## API ETL

### PostgreSQL

| Method | Endpoint | Fungsi |
| --- | --- | --- |
| POST | `/api/v1/etl/postgres/regions/{region_id}?days=1` | Load satu kabupaten/kota ke PostgreSQL |
| POST | `/api/v1/etl/postgres/provinces/{province_id}?days=1` | Load seluruh kabupaten/kota dalam satu provinsi ke PostgreSQL |

### MySQL

| Method | Endpoint | Fungsi |
| --- | --- | --- |
| POST | `/api/v1/etl/mysql/regions/{region_id}?days=1` | Load satu kabupaten/kota ke MySQL |
| POST | `/api/v1/etl/mysql/provinces/{province_id}?days=1` | Load seluruh kabupaten/kota dalam satu provinsi ke MySQL |

Parameter `days` menerima nilai `1–5`. Forecast tetap otomatis mengambil 3 hari ke depan.

## API Overview

| Method | Endpoint | Fungsi |
| --- | --- | --- |
| GET | `/api/v1/overview/{region_id}?days=5` | Menampilkan weather historical dan ringkasan hotspot dari PostgreSQL |

Contoh:

```bash
curl "http://127.0.0.1:8002/api/v1/overview/61.01?days=5"
```

## Yang Sudah Selesai

- ETL weather, hotspot, air quality, dan forecast.
- Load ke PostgreSQL dan MySQL.
- Upsert dan unique key untuk mencegah duplikasi.
- Airflow lokal dengan scheduler otomatis.
- Pipeline Kalimantan untuk 5 provinsi.
- Retry otomatis saat sumber data atau database sementara gagal.
- Tabel `tb_r_etl_run` pada PostgreSQL dan MySQL.
- Endpoint overview dari PostgreSQL.

## Yang Belum

- Notifikasi email/WhatsApp saat DAG gagal.
- Data-quality check otomatis di DAG.
- Monitoring dashboard khusus pipeline.
- Penyimpanan raw data sebelum transformasi.
- Dokumentasi data dictionary lengkap per tabel.
- Deployment Airflow dan FastAPI ke server production.

## Struktur Folder

```text
.
├── app/
│   ├── api/routes/             # Endpoint FastAPI
│   ├── database/
│   │   ├── postgres/           # Koneksi, loader, dan SQL PostgreSQL
│   │   └── mysql/              # Koneksi, loader, dan SQL MySQL
│   ├── pipelines/             # Orkestrasi load per region/provinsi
│   ├── services/              # Client dan transform data tiap sumber
│   └── config.py              # Pembacaan variabel environment
├── airflow/
│   ├── dags/                  # DAG PostgreSQL dan MySQL
│   ├── Dockerfile             # Image Airflow lokal
│   └── docker-compose.yml     # Webserver, scheduler, dan metadata Airflow
├── main.py                     # Entrypoint FastAPI
├── requirements.txt            # Dependensi API/pipeline
└── .env.example                # Template konfigurasi lokal
```

## Prasyarat

- Python 3.9 atau lebih baru.
- Docker Desktop (hanya untuk Airflow lokal).
- Akses ke database PostgreSQL dan/atau MySQL yang sudah memiliki tabel data karhutla.
- API key Visual Crossing dan NASA FIRMS (`MAP_KEY`).

Database target dipatok oleh kode sebagai berikut:

- PostgreSQL: `environmental_conditions`.
- MySQL: `enviromental_conditions`.

Sebelum menjalankan DAG, buat tabel pencatatan run dengan skrip yang tersedia:

```bash
psql -h <postgres_host> -U <postgres_user> -d environmental_conditions \
  -f app/database/postgres/tb_r_etl_run.sql

mysql -h <mysql_host> -u <mysql_user> -p enviromental_conditions \
  < app/database/mysql/tb_r_etl_run.sql
```

## Konfigurasi

Salin template environment lalu isi seluruh nilai sesuai kredensial yang digunakan.

```bash
cp .env.example .env
```

```dotenv
VISUAL_CROSSING_API_KEY=...
MAP_KEY=...

POSTGRESQL_HOST=...
POSTGRESQL_PORT=5432
POSTGRESQL_USER=...
POSTGRESQL_PASSWORD=...

MYSQL_HOST=...
MYSQL_PORT=3306
MYSQL_USER=...
MYSQL_PASSWORD=...
```

Jangan commit file `.env` karena berisi API key dan kredensial database.

## Menjalankan API Lokal

1. Buat virtual environment dan pasang dependensi.

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Pastikan `.env` sudah diisi, lalu jalankan FastAPI.

   ```bash
   uvicorn main:app --host 127.0.0.1 --port 8002 --reload
   ```

3. Akses API di `http://127.0.0.1:8002` atau dokumentasi interaktif di `http://127.0.0.1:8002/docs`.

Contoh trigger ETL PostgreSQL untuk satu kabupaten/kota:

```bash
curl -X POST "http://127.0.0.1:8002/api/v1/etl/postgres/regions/61.01?days=1"
```

## Menjalankan Airflow Lokal

Airflow memakai PostgreSQL container `airflow-db` hanya untuk metadata Airflow. Data karhutla tetap dimuat ke database target yang dikonfigurasi di `.env` root proyek.

1. Pastikan Docker Desktop aktif dan file `.env` telah terisi.

2. Inisialisasi metadata dan akun Airflow dari root proyek.

   ```bash
   docker compose -f airflow/docker-compose.yml run --rm airflow-init
   ```

3. Jalankan webserver dan scheduler.

   ```bash
   docker compose -f airflow/docker-compose.yml up -d
   ```

4. Buka `http://localhost:8080`, lalu masuk menggunakan akun `admin` / `admin`. Aktifkan atau trigger DAG `load_kalimantan_postgres` dan `load_kalimantan_mysql` dari UI bila diperlukan.

Untuk menghentikan layanan:

```bash
docker compose -f airflow/docker-compose.yml down
```

Gunakan `down -v` hanya bila ingin menghapus metadata Airflow dan seluruh riwayat run lokal.
