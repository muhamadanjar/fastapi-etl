# ETL Data Workspace

Konteks ini mengelola asal data, data mentah yang dikumpulkan, dan pembentukan data baru dari satu atau lebih kumpulan data.

## Language

**Dataset**:
Snapshot immutable berisi kumpulan record dengan sebuah schema. Dataset dapat berupa Raw Dataset atau Derived Dataset; dalam MVP ukurannya dibatasi hingga 100.000 record atau 100 MB data terurai.
_Avoid_: Table, batch, result set

**Dataset Schema**:
Daftar nama kolom dan tipe data yang melekat pada satu Dataset.
_Avoid_: Table definition, mapping config

**Schema Drift**:
Perubahan Dataset Schema yang membuat input terbaru tidak kompatibel dengan Data Recipe. Schema Drift harus terlihat dan tidak diperbaiki otomatis.
_Avoid_: Mapping error, bad data

**Data Source**:
Asal data logis berstatus ACTIVE atau INACTIVE, berversi, dan dapat diambil kembali. Dalam MVP, sebuah Data Source berupa File Source atau API Source; Data Source yang tidak lagi digunakan dinonaktifkan tanpa menghapus histori Dataset-nya.
_Avoid_: Source type, input, connection

**File Source**:
Data Source logis yang menerima satu atau lebih Source Artifact dari Upload API dari waktu ke waktu.
_Avoid_: Upload job, file job

**Source Artifact**:
Referensi immutable ke file CSV, Excel, atau JSON yang dimiliki Upload API dan digunakan untuk satu Ingestion.
_Avoid_: Uploaded file, local file, file path

**API Source**:
Data Source yang mengambil array record JSON melalui satu request HTTP GET deklaratif, dengan optional page-number pagination.
_Avoid_: API job, remote job

**Source Credential**:
Secret milik API Source untuk autentikasi tanpa autentikasi, API key header, atau bearer token statis. Nilainya dapat diganti tetapi tidak dapat dibaca kembali.
_Avoid_: User credential, connection config

**Raw Dataset**:
Snapshot immutable dan lengkap yang dihasilkan dari satu kali pengambilan Data Source. Pengambilan ulang selalu menghasilkan Raw Dataset baru dan tidak menimpa snapshot sebelumnya; ingestion yang gagal tidak menghasilkan Raw Dataset parsial.
_Avoid_: Batch, upload result, raw table

**Raw Record**:
Satu record asli di dalam tepat satu Raw Dataset, dipertahankan sebagaimana diterima dari sumbernya.
_Avoid_: Row, job record

**Ingestion**:
Pengambilan eksplisit isi Data Source untuk menghasilkan Raw Dataset baru.
_Avoid_: Extract job, sync job

**Ingestion Run**:
Catatan satu pelaksanaan Ingestion yang menyimpan snapshot konfigurasi source non-secret, referensi versi credential, status, statistik, dan kegagalannya. File Source memulainya setelah artifact didaftarkan, sedangkan API Source dimulai atas permintaan pengguna.
_Avoid_: Job, task

**Run**:
Catatan immutable sebuah pelaksanaan Ingestion atau Data Recipe dengan status PENDING, RUNNING, SUCCEEDED, atau FAILED. Menjalankan ulang selalu membuat Run baru.
_Avoid_: Job execution, retry attempt

**Join**:
Penggabungan kolom dari tepat dua Dataset berdasarkan satu atau beberapa key yang sama. MVP mendukung inner join dan left join, mempertahankan hasil perkalian record dari key duplikat, dan tidak mencocokkan key null; penggabungan lebih banyak Dataset menggunakan Recipe Chain.
_Avoid_: Merge, match

**Union**:
Penggabungan record dari 2–10 Dataset yang nama, urutan, dan tipe kolomnya identik setelah Column Mapping, tanpa coercion atau pengisian kolom otomatis.
_Avoid_: Merge, append job

**Derived Dataset**:
Dataset immutable dan lengkap yang dihasilkan oleh Transformation Run sukses tanpa mengubah dataset sumber. Derived Dataset dapat dilihat, diunduh, atau digunakan sebagai input Data Recipe lain.
_Avoid_: Processed table, job output

**Data Recipe**:
Definisi reusable, berversi, dan berstatus ACTIVE atau INACTIVE untuk membentuk Derived Dataset dari satu atau lebih input Dataset menggunakan operasi Join atau Union, mapping kolom, filter, dan bentuk output. Input dari Data Source default ke Raw Dataset terbaru tetapi dapat dipilih secara eksplisit.
_Avoid_: Job, ETL job, workflow

**Transformation Run**:
Catatan satu pelaksanaan Data Recipe yang menyimpan snapshot definisi recipe, versinya, setiap Dataset input yang benar-benar digunakan, status, statistik, dan kegagalannya. Run sukses menghasilkan tepat satu Derived Dataset; run gagal tidak menghasilkan output parsial.
_Avoid_: Job execution, task

**Column Mapping**:
Pemilihan, pembuangan, penggantian nama, atau konversi tipe dasar sebuah kolom dalam Data Recipe.
_Avoid_: Field rule, transformation rule

**Dataset Filter**:
Kondisi deklaratif sederhana untuk menentukan record yang disertakan dalam Derived Dataset.
_Avoid_: Quality rule, validation rule

**Recipe Chain**:
Hubungan ketika Derived Dataset dari satu Data Recipe digunakan sebagai input Data Recipe lain. Setiap tahap dijalankan secara eksplisit, dan rantai tidak boleh membentuk siklus.
_Avoid_: Job dependency, workflow orchestration

**Actor**:
Pengguna atau service yang identitas dan izinnya ditetapkan oleh User Management serta tercatat saat melakukan perubahan pada ETL.
_Avoid_: Local user, ETL user

**Dataset Lineage**:
Hubungan asal-usul tingkat dataset yang mengaitkan Data Source dengan Raw Dataset serta input Dataset dan Data Recipe dengan Derived Dataset.
_Avoid_: Record lineage, entity history, change log
