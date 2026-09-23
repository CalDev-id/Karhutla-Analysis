# Airflow lokal

Airflow menjalankan penjadwalan pipeline. Database `airflow-db` di Compose hanya
untuk metadata Airflow; data karhutla tetap dimuat ke PostgreSQL proyek yang
konfigurasinya berada di `.env` pada root proyek.

## Menjalankan

1. Buka Docker Desktop dan tunggu sampai statusnya berjalan.
2. Dari root proyek, inisialisasi metadata dan akun Airflow:

   ```bash
   docker compose -f airflow/docker-compose.yml run --rm airflow-init
   ```

3. Nyalakan webserver dan scheduler:

   ```bash
   docker compose -f airflow/docker-compose.yml up -d
   ```

4. Buka http://localhost:8080 lalu login dengan `admin` / `admin`.

## DAG gabungan

`load_kalimantan_combined` berjalan setiap hari pukul **09:00 WIB** untuk provinsi
Kalimantan `61`–`65`. DAG ini menyimpan master wilayah ke MySQL `region` dan data
environmental ke PostgreSQL `environment_conditions`.

Secara default DAG memuat **5 hari terakhir**. Ubah Airflow Variable
`KARHUTLA_ETL_DAYS` ke nilai `1`–`5` melalui **Admin → Variables** bila perlu.

Untuk menghentikan DAG lama dan mengaktifkan DAG gabungan, jalankan dari root proyek:

```bash
docker compose -f airflow/docker-compose.yml exec airflow-webserver \
  airflow dags pause load_kalimantan_postgres
docker compose -f airflow/docker-compose.yml exec airflow-webserver \
  airflow dags pause load_kalimantan_mysql
docker compose -f airflow/docker-compose.yml exec airflow-webserver \
  airflow dags unpause load_kalimantan_combined
```

Alternatifnya, pada halaman **DAGs** di Airflow, matikan toggle untuk
`load_kalimantan_postgres` dan `load_kalimantan_mysql`, kemudian hidupkan toggle
`load_kalimantan_combined`.

## Menghentikan

```bash
docker compose -f airflow/docker-compose.yml down
```

Gunakan `down -v` hanya bila ingin menghapus metadata Airflow dan seluruh riwayat run lokal.
