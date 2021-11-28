#!/usr/bin/env python3

import requests
import pandas as pd
import numpy as np
import itertools

urls = [
        "https://www.bcliquorstores.com/ajax/browse?category=wine&sort=name.raw:asc&size=5000",
        "https://www.bcliquorstores.com/ajax/browse?category=beer&sort=name.raw:asc&size=5000",
        "https://www.bcliquorstores.com/ajax/browse?category=spirits&sort=name.raw:asc&size=5000",
        "https://www.bcliquorstores.com/ajax/browse?category=coolers/ciders&sort=name.raw:asc&size=5000"
        ]

items = list(itertools.chain(*[requests.get(url).json()["hits"]["hits"] for url in urls]))

df_items = pd.DataFrame(items)
df_exploded = pd.DataFrame(df_items['_source'].tolist())
df = df_items.merge(df_exploded, left_on="_id", right_on="sku")

# Explode some more columns
df['category_id'] = df['category'].apply(lambda cell: cell['id'])
df['category'] = df['category'].apply(lambda cell: cell['description'])
df['subCategory_id'] = df['subCategory'].apply(lambda cell: cell['id'])
df['subCategory'] = df['subCategory'].apply(lambda cell: cell['description'])
df['class_id'] = df['class'].apply(lambda cell: cell['id'])
df['class'] = df['class'].apply(lambda cell: cell['description'])
df['sort'] = df['sort'].apply(lambda cell: cell[0])

# Convert columns
df['alcoholPercentage'] = df['alcoholPercentage'].apply(lambda cell: cell/100)
df['regularPrice'] = df['regularPrice'].apply(lambda cell: float(cell))
# Convert to float
for col in ['volume', 'regularPrice', '_currentPrice', '_regularPrice', '_score', 'currentPrice']:
    df[col] = df[col].apply(lambda cell: float(cell))

# Composite columns

# Total volume
df['totalVolume'] = df['unitSize'] * df['volume'] * df['availableUnits']
# Total value
df['totalPrice'] = df['availableUnits'] * df['regularPrice']
# Total alcohol content in L
df['totalAlcoholContent'] = df['unitSize'] * df['volume'] * df['alcoholPercentage']
# Price per alcohol content in L
df['alcoholCost'] = df['regularPrice'] / df['totalAlcoholContent']
# Hardly legit, but cost by volume weighted by rating
#df['costPerRating'] = df['regularPrice'] / (df['unitSize'] * df['volume']) / df['consumerRating']
# Store percentage
df['storePercent'] = df['storeCount'] / df['storeCount'].max()

# Drop columns
df = df.drop(columns=['_currentPrice', '_regularPrice', '_source'])

df.to_csv("bcliquordata.csv")
df.to_json("bcliquordata.json", orient='records', lines=True)
