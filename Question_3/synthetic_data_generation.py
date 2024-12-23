# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:06:12 2024

@author: odanielz
"""


import os
import pandas as pd
from faker import Faker
import numpy as np
from datetime import timedelta, datetime

# Initialize Faker and constants
fake = Faker()
rows_per_file = 1_000_000  # 1 million rows per file
days = 15  # 15 days of logs

# Define output directory
output_dir = "task_logs"
os.makedirs(output_dir, exist_ok=True)

# Generate synthetic task log data
def generate_task_log_data(rows, date):
    return {
        'task_id': [fake.uuid4() for _ in range(rows)],
        'user_id': [fake.uuid4() for _ in range(rows)],
        'task_description': [fake.sentence(nb_words=6) for _ in range(rows)],
        'start_time': [date + timedelta(seconds=np.random.randint(0, 86400)) for _ in range(rows)],
        'end_time': [date + timedelta(seconds=np.random.randint(86400, 172800)) for _ in range(rows)],
        'status': [np.random.choice(['Completed', 'In Progress', 'Failed']) for _ in range(rows)],
        'priority': [np.random.choice(['Low', 'Medium', 'High']) for _ in range(rows)],
        'hours_logged': [round(np.random.uniform(0.5, 12), 2) for _ in range(rows)],
    }

# Generate and write Parquet files
for file_num, day in enumerate(range(days)):
    current_date = datetime(2024, 12, 1) + timedelta(days=day)

    print(f"Generating file for {current_date.date()} - File {file_num + 1}")
    data = generate_task_log_data(rows_per_file, current_date)
    df = pd.DataFrame(data)
    
    # Convert datetime columns to milliseconds
    df['start_time'] = pd.to_datetime(df['start_time']).astype('int64') // 10**6  # Convert to milliseconds
    df['end_time'] = pd.to_datetime(df['end_time']).astype('int64') // 10**6  # Convert to milliseconds

    # Write to Parquet
    output_file = os.path.join(output_dir, f"task_log_{current_date.date()}_file_{file_num + 1}.parquet")
    df.to_parquet(output_file, index=False)

    print(f"File {file_num + 1} written to {output_file}")
    
