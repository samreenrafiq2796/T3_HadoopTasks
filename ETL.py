from hdfs import InsecureClient
import pandas as pd

client = InsecureClient('http://127.0.0.1:50070', user='hdfs')

with client.read('/user/maria_dev/T3Task/penguins.csv', encoding='utf-8') as file:
    df = pd.read_csv(file)

print("First five records:")
display(df.head())

print("Dataset information:")
df.info()

print("\nDataset shape:", df.shape)

print("\nDataset statistics:")
display(df.describe())

print("\nMissing values:")
print(df.isnull().sum())

print("\nMissing values in selected columns:")
print(df[['bill_length_mm', 'flipper_length_mm',
          'body_mass_g', 'sex']].isnull().sum())

print("\nSpecies and island counts:")
display(df.groupby(['species', 'island']).size().reset_index(name='count'))

print("\nAverage body mass and flipper length:")
display(df.groupby('species')[['body_mass_g',
                               'flipper_length_mm']].mean().round(2))

                               print("Missing values before cleaning:")
print(df.isnull().sum())


df = df.dropna()


df['body_mass_kg'] = df['body_mass_g'] / 1000

print("\nMissing values after cleaning:")
print(df.isnull().sum())

display(df.head())

print("\nBody mass in kilograms:")
display(df[['species', 'body_mass_g', 'body_mass_kg']].head())

print("\nDataset shape after cleaning:", df.shape)

hdfs_path = '/user/maria_dev/T3Task/penguins_clean.csv'


with client.write(hdfs_path, encoding='utf-8', overwrite=True) as file:
    df.to_csv(file, index=False)

print("Cleaned dataset uploaded successfully!")

print("\nFiles in HDFS directory:")
print(client.list('/user/maria_dev/T3Task'))