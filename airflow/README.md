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

## Menghentikan

```bash
docker compose -f airflow/docker-compose.yml down
```

Gunakan `down -v` hanya bila ingin menghapus metadata Airflow dan seluruh riwayat run lokal.
