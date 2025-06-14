import pandas as pd
from pathlib import Path
from Chapter2.myCreateDataset import myCreateDataset
from util.VisualizeDataset import VisualizeDataset
from util import util
import copy
import os
import sys

RESULT_PATH = Path('./intermediate_datafiles/')
RESULT_FNAME = 'resultsWOactivity.csv'

print("Please wait, this will take a while to run!")

datasets = []

for activity in ['cycling', 'dance', 'running', 'sitting', 'walking']:
    DATASET_PATH = Path(f'./datasets/mlqs/{activity}')  # 🔁 each folder
    print(f"Creating numerical datasets from files of the activity: {activity}")

    dataset = myCreateDataset(DATASET_PATH, 1000)

    dataset.add_numerical_dataset('Accelerometer.csv', 'time', ['z', 'y', 'x'], 'avg', 'acc_')
    dataset.add_numerical_dataset('Gyroscope.csv', 'time', ['z', 'y', 'x'], 'avg', 'gyro_')
    dataset.add_numerical_dataset('Gravity.csv', 'time', ['z', 'y', 'x'], 'avg', 'grav_')

    dataset.add_numerical_dataset('WristMotion.csv','time',
        [
        'rotationRateX', 'rotationRateY', 'rotationRateZ',
        'gravityX', 'gravityY', 'gravityZ',
        'accelerationX', 'accelerationY', 'accelerationZ',
        'quaternionW', 'quaternionX', 'quaternionY', 'quaternionZ'
    ],
    'avg', 'wrist_')

    dataset.add_numerical_dataset('WatchBarometer.csv','time', ['relativeAltitude', 'pressure'], 'avg', 'baro_')
    dataset.add_numerical_dataset('HeartRate.csv', 'time', ['bpm'], 'avg', 'heart_')

    df = dataset.data_table.copy()
    #df['activity'] = activity
    datasets.append(df)

    DataViz = VisualizeDataset(__file__)
    DataViz.plot_dataset_boxplot(df, ['acc_x','acc_y','acc_z','wrist_accelerationX','wrist_accelerationY','wrist_accelerationZ'])

    util.print_statistics(df)

# Combine all activities
final_df = pd.concat(datasets)

# Compare two activities (commented this because I added activity (string))
#util.print_latex_table_statistics_two_datasets(datasets[0], datasets[1])

# Save
final_df.to_csv(RESULT_PATH / RESULT_FNAME)

print('The code has run through successfully!')
