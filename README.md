# Music Streaming Analysis Using Spark Structured APIs

This repository solves the **Hands‑on L6: Spark Structured API** assignment by analyzing user listening behavior and music trends with PySpark.

## Datasets
- `inputs/listening_logs.csv` – user plays with `user_id, song_id, timestamp, duration_sec`
- `inputs/songs_metadata.csv` – song metadata with `song_id, title, artist, genre, mood`

Generate fresh inputs (≥100 records) with:
```bash
python3 datagen.py
```

## Tasks Implemented
1. **User favourite genre** – most‑played genre per user (ties broken alphabetically).
2. **Average listen time per song** – mean `duration_sec` grouped by song.
3. **Genre loyalty score** – favourite‑genre plays / total plays; includes a subfolder with users > 0.8.
4. **Night owl users** – users who **frequently** listen between **00:00–05:00**. Here, “frequently” means **≥5 night plays and ≥30%** of that user’s total plays.

## Output Structure
```
outputs/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── genre_loyalty_scores/
│   └── above_0_8/
└── night_owl_users/
    ├── all_user_night_stats/
    └── frequent/
```

## How to Run (Local or Codespaces)
1) (Optional) Generate inputs:
```bash
python3 datagen.py
```
2) Run with Spark:
```bash
spark-submit main.py --input_dir inputs --output_dir outputs
```
3) Inspect results:
```bash
ls -R outputs/
```

## Notes
- The pipeline uses only Spark Structured APIs (DataFrames + Window functions).
- CSV outputs are written with headers and `mode=overwrite` for reproducibility.
- Time window for night activity: **[00:00, 05:00)** (hours 0–4).

---
