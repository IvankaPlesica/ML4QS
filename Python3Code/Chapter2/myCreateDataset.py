import pandas as pd
import numpy as np
import copy
import re
from datetime import timedelta

class myCreateDataset:

    base_dir = ''
    granularity = 0
    data_table = None

    def __init__(self, base_dir, granularity):
        self.base_dir = base_dir
        self.granularity = granularity
    
    def create_timestamps(self, start_time, end_time):
        return pd.date_range(start_time, end_time, freq=str(self.granularity)+'ms')
    
    def create_dataset(self, start_time, end_time, cols, prefix):
        c = copy.deepcopy(cols)

        if not prefix == '':
            for i in range(0, len(c)):
                c[i] = str(prefix) + str(c[i])

        timestamps = self.create_timestamps(start_time, end_time)

        #Specify the datatype here to prevent an issue
        self.data_table = pd.DataFrame(index=timestamps, columns=c, dtype=object)
    
    def add_numerical_dataset(self, file, timestamp_col, value_cols, aggregation='avg', prefix=''):
        print(f'Reading data from {file}')
        dataset = pd.read_csv(self.base_dir / file, skipinitialspace=True)

        #Convert time to date 
        dataset[timestamp_col] = pd.to_datetime(dataset[timestamp_col])

                # Create a table based on the times found in the dataset
        if self.data_table is None:
            self.create_dataset(min(dataset[timestamp_col]), max(dataset[timestamp_col]), value_cols, prefix)
        else:
            for col in value_cols:
                self.data_table[str(prefix) + str(col)] = np.nan
        
        if 'seconds_elapsed' in dataset.columns:
            self.data_table['seconds_elapsed'] = np.nan

        # Over all rows in the new table
        for i in range(0, len(self.data_table.index)):
            # Select the relevant measurements.
            relevant_rows = dataset[
                (dataset[timestamp_col] >= self.data_table.index[i]) &
                (dataset[timestamp_col] < (self.data_table.index[i] +
                                           timedelta(milliseconds=self.granularity)))
            ]
            
            if 'seconds_elapsed' in dataset.columns:
                if len(relevant_rows) > 0:
                    self.data_table.loc[self.data_table.index[i], 'seconds_elapsed'] = np.average(relevant_rows['seconds_elapsed'])
                else:
                    self.data_table.loc[self.data_table.index[i], 'seconds_elapsed'] = np.nan
            
            for col in value_cols:
                # Take the average value
                if len(relevant_rows) > 0:
                    if aggregation == 'avg':
                        self.data_table.loc[self.data_table.index[i], str(prefix)+str(col)] = np.average(relevant_rows[col])
                    else:
                        raise ValueError(f"Unknown aggregation {aggregation}")
                else:
                    self.data_table.loc[self.data_table.index[i], str(prefix)+str(col)] = np.nan

    