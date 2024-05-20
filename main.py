#Main file with a function which takes the file name and creates log and clean data CSV files

import pandas as pd
import numpy as np
from datetime import datetime


def clean_data(filename):
    df = pd.read_csv(filename, sep='\t')

    #Converting the dataset into vertical mode
    df['participants'] = df['participants'].str.split('|')
    df['participants_price'] = df['participants_price'].str.split('|')

    # Create new column with tuples
    df['combined'] = df.apply(lambda row: list(zip(row['participants'], row['participants_price'])), axis=1)

    # Explode 'combined' column
    df = df.explode('combined')

    # Create separate columns from 'combined'
    df['participants'], df['participants_price'] = zip(*df['combined'])

    # Drop 'combined' column
    df = df.drop(columns=['combined'])

    df['participants'] = df['participants'].str.strip()
    df['participants_price'] = df['participants_price'].str.strip()

    #Winner price should be float
    mask = df['winner_price'].apply(lambda x: isinstance(x, float))
    df1 = df[mask]
    df_incorrectos = df[~mask]

    #Check NaN/NotNull
    mask = df1['winner_price'].apply(lambda x: x is not np.nan)
    df2 = df1[mask]

    #Always appending the new incorrect data into a dataframe with all the wrong rows
    df_incorrectos=pd.concat([df_incorrectos, df1[~mask]])

    #Winner price in range
    mask = df2['winner_price'].apply(lambda x: 0 <= x <= 1)
    df3 = df2[mask]
    df_incorrectos=pd.concat([df_incorrectos, df3[~mask]])

    #Winner price <= participant_price
    mask = df3.apply(lambda row: float(row['winner_price']) <= float(row['participants_price']), axis=1)
    df4 = df3[mask]
    df_incorrectos=pd.concat([df_incorrectos, df3[~mask]])
    
    #Winner price <= maximum price allowed
    mask = df4.apply(lambda row: row['winner_price'] <= row['maximum_price_allowed'], axis=1)
    df5 = df4[mask]
    df_incorrectos=pd.concat([df_incorrectos, df4[~mask]])

    #Maximum price allowed in range
    mask = df5['maximum_price_allowed'].apply(lambda x: 0 <= x <= 1)
    df6 = df5[mask]
    df_incorrectos=pd.concat([df_incorrectos, df5[~mask]])

    #Log wrong data
    now = datetime.now()
    timestamp_str = now.strftime("%Y_%m_%d%H_%M_%S")
    df_incorrectos['execution_time'] = timestamp_str
    filename = f"error_log_{timestamp_str}.csv"
    df_incorrectos.to_csv(filename, mode='a', header=False)

    #Write to csv clean data
    filename = f"clean_data_{timestamp_str}.csv"
    df6.to_csv(filename, mode='a', header=True)