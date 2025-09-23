# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project demonstrates the use of **Apache Spark Structured APIs** to perform **scalable data analysis** on synthetic music streaming data.  
The goal is to derive insights into user listening behavior and music trends by processing structured logs and metadata using Spark. The expected outcome is a set of structured insights into genre preferences, song popularity, and engagement patterns.

---

## Learning Objectives
1. **Data Loading and Preparation**  
   - Import large structured datasets into Spark DataFrames.  
   - Handle schema definitions, data types, and timestamp conversions.  

2. **Data Analysis with Spark Structured APIs**  
   - Apply filtering, aggregation, joins, and transformations.  
   - Use window functions for per-user ranking and score computation.  

3. **Result Exporting**  
   - Persist processed results into partitioned directories in `.csv` or `.json` format.  
   - Ensure reproducibility with structured output folders.  

4. **Practical Skills**  
   - Develop hands-on familiarity with **Spark SQL, DataFrames, and window functions**.  
   - Build and execute a Spark workflow end-to-end in a **cloud-based environment (GitHub Codespaces)**.  

---

## Dataset Description

The dataset consists of two CSV files generated via the provided **synthetic data generator (`datagen.py`)**.  
Both files contain at least **100 records** to guarantee sufficient variety for meaningful analysis.

### 1. listening_logs.csv
- `user_id` – unique identifier for the user  
- `song_id` – unique identifier for the song played  
- `timestamp` – date and time when the song was played (e.g., `2025-03-23 14:05:00`)  
- `duration_sec` – duration (in seconds) that the user listened to the song  

### 2. songs_metadata.csv
- `song_id` – unique identifier for the song  
- `title` – title of the track  
- `artist` – performing artist  
- `genre` – musical genre (Pop, Rock, Jazz, Classical, Hip-Hop, etc.)  
- `mood` – emotional tag of the track (Happy, Sad, Energetic, Chill, etc.)  

---

## Repository Structure
```
.
├── datagen.py              # Synthetic dataset generator
├── main.py                 # Spark pipeline implementation
├── README.md               # Project documentation and results
├── requirements.txt        # Python dependencies
├── inputs/                 # Input datasets
│   ├── listening_logs.csv
│   └── songs_metadata.csv
└── outputs/                # Output results from Spark
```

---

## Output Directory Structure
```
outputs/
├── user_favorite_genres/           # Task 1 results
├── avg_listen_time_per_song/       # Task 2 results
├── genre_loyalty_scores/           # Task 3 results (all users)
│   └── above_0_8/                  # Subset with loyalty > 0.8
└── night_owl_users/                # Task 4 results
    ├── all_user_night_stats/
    └── frequent/
```

---

## Tasks, Methodology, and Sample Outputs

### Task 1 – User’s Favourite Genre
**Objective:** Identify the most frequently played genre for each user.  
**Method:**  
- Join `listening_logs` with `songs_metadata` on `song_id`.  
- Count plays grouped by `user_id` and `genre`.  
- Use **window functions** (`row_number()`) to select the top genre per user.  

**Sample Output:**
```
user_id,genre,plays
user_1,Rock,12
user_2,Pop,15
user_3,Jazz,7
```

---

### Task 2 – Average Listen Time per Song
**Objective:** Compute the average listening duration for each song.  
**Method:**  
- Group by `song_id`, `title`, `artist`, `genre`.  
- Aggregate with `avg(duration_sec)`.  

**Sample Output:**
```
song_id,title,artist,genre,avg_listen_time_sec
song_1,Title_song_1,Artist_3,Pop,180.5
song_2,Title_song_2,Artist_7,Rock,210.3
```

---

### Task 3 – Genre Loyalty Score
**Objective:** Measure how strongly a user prefers their favorite genre.  
**Formula:**  
	eq loyalty_score = (plays in favorite genre) / (total plays)  

**Method:**  
- Compute total plays per user.  
- Compute plays per user-genre.  
- Identify each user’s top genre.  
- Calculate loyalty ratio and filter users with score > 0.8.  

**Sample Output (all users):**
```
user_id,fav_genre,fav_genre_plays,total_plays,loyalty_score
user_1,Rock,12,15,0.80
user_2,Pop,20,22,0.91
```

---

### Task 4 – Night Owl Users
**Objective:** Detect users who frequently listen between **00:00–05:00**.  
**Criteria:**  
- At least **5 plays** in the time window.  
- At least **30% of their total plays**.  

**Method:**  
- Extract hour from `timestamp`.  
- Filter plays where `0 <= hour < 5`.  
- Compute ratio of night plays vs total plays per user.  

**Sample Output:**
```
user_id,night_plays,total_plays,night_ratio
user_5,8,20,0.40
user_8,12,30,0.40
```

---

## Execution Instructions

### Prerequisites
1. **Python 3.x**
   ```bash
   python3 --version
   ```

2. **PySpark**
   ```bash
   pip install pyspark
   ```

3. **Apache Spark**
   - [Download Spark](https://spark.apache.org/downloads/)  
   - Verify installation:  
     ```bash
     spark-submit --version
     ```

---

### Step 1 – Generate Input Data
```bash
python3 datagen.py
```

### Step 2 – Run the Analysis
```bash
spark-submit main.py --input_dir inputs --output_dir outputs
```

### Step 3 – Verify Outputs
```bash
ls -R outputs/
```

---

## Common Errors and Resolutions
- **`ModuleNotFoundError: No module named 'pyspark'`**  
  → Run `pip install pyspark`  

- **`spark-submit: command not found`**  
  → Ensure Spark is installed and added to PATH  

- **Permission denied while writing outputs**  
  → Delete old outputs or adjust directory permissions  

---


## References
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)  
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)  
- Course GitHub: [Assignment Instructions](https://github.com/ITCS6190-Fall2025/h6_itcs6190_Spark_Structured_APIs_Music_Listener_Behaviour_Analysis)

---
