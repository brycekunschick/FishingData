import pandas as pd

df_fish = pd.read_csv('fishing.csv')

df_fish['CatchDateTime'] = pd.to_datetime(df_fish['Date'] + ' ' + df_fish['Time'])

df_fish['CatchDateTime'] = df_fish['CatchDateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

df_fish = df_fish.drop(columns=['Date', 'Time'])

# Save new CSV with datetime column:
df_fish.to_csv('data/catch_data.csv', index=False)