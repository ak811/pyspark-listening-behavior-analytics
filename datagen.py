#!/usr/bin/env python3
"""
Generate synthetic inputs for the Music Listener Behaviour Analysis assignment.

Creates:
  ./inputs/listening_logs.csv
  ./inputs/songs_metadata.csv
"""
import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path

def main(output_dir: str = "inputs", seed: int = 42, num_users: int = 100, num_songs: int = 50, num_logs: int = 1000):
    random.seed(seed)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Generate listening_logs.csv
    user_ids = [f'user_{i}' for i in range(1, num_users+1)]
    song_ids = [f'song_{i}' for i in range(1, num_songs+1)]

    logs = []
    start_date = datetime(2025, 3, 1)
    end_date = datetime(2025, 3, 28)

    for _ in range(num_logs):
        logs.append([
            random.choice(user_ids),
            random.choice(song_ids),
            (start_date + timedelta(seconds=random.randint(0, 
                int((end_date - start_date).total_seconds())))).strftime('%Y-%m-%d %H:%M:%S'),
            random.randint(30, 300)
        ])

    pd.DataFrame(logs, columns=['user_id','song_id','timestamp','duration_sec'])\
      .to_csv(out / 'listening_logs.csv', index=False)

    # Generate songs_metadata.csv
    genres = ['Pop', 'Rock', 'Jazz', 'Classical', 'Hip-Hop']
    moods = ['Happy', 'Sad', 'Energetic', 'Chill']

    metadata = []
    for song_id in song_ids:
        metadata.append([
            song_id,
            f'Title_{song_id}',
            f'Artist_{random.randint(1, 20)}',
            random.choice(genres),
            random.choice(moods)
        ])

    pd.DataFrame(metadata, columns=['song_id','title','artist','genre','mood'])\
      .to_csv(out / 'songs_metadata.csv', index=False)

if __name__ == "__main__":
    main()
