# Sakila Datawarehouse Project
## Deskripsi
Project ini bertujuan untuk membuat datawarehouse  onpremise menggukan database sakila. Pada project ini dilakukan perubahan data model dari snowflake schema menjadi star schema untuk memudahkan proses query data di dalam datawarehouse. Python digunakan pada project ini untuk melakukan proses ETL dan untuk datawarehouse-nya digunakan MySQL.

## Tujuan
1.	Melakukan proses ETL dari database sakila
2.	Membuat datawarehouse dengan menggunakan Star Schema

## Cara menggunakan kode.
1. Download repository ke komputer lokal
2. Buat Environment Python. Dapat menggunakan perintah:
    `python -m venv nama_virt_env`
3. Install dependensi yang tersimpan di requirements.txt
4. Buat file sql_params.yml didalam folder query. Variabel yang perlu dimasukkan ke dalam file sql_params:
    - "host":"nama_host",
    - "user":"user_login",
    - "password":"password",
    - "db_extract":"nama_db_souce",
    - "db_load":"nama_db_target"
5. Buat Folder **log_data/**
6. Jalankan file run.py

## Workflow

## Hasil
![alt text](https://github.com/Muhammadfaizaladdi/Sakila-DWH/main/pictures/DB Schema.jpg?raw=true)
