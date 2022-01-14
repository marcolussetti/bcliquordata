#!/usr/bin/env python3

import requests
import pandas as pd
import numpy as np
import itertools
from datetime import datetime
import os

urls = [
        "https://www.bcliquorstores.com/ajax/browse?category=wine&sort=name.raw:asc&size=5000",
        "https://www.bcliquorstores.com/ajax/browse?category=beer&sort=name.raw:asc&size=5000",
        "https://www.bcliquorstores.com/ajax/browse?category=spirits&sort=name.raw:asc&size=5000",
        "https://www.bcliquorstores.com/ajax/browse?category=coolers/ciders&sort=name.raw:asc&size=5000"
        ]

items = list(itertools.chain(*[requests.get(url).json()["hits"]["hits"] for url in urls]))

current_time = datetime.now()

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
# Convert to float
for col in ['volume', 'regularPrice', '_currentPrice', '_regularPrice', '_score', 'currentPrice']:
    df[col] = df[col].apply(lambda cell: float(cell) if cell else np.nan)

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
df['costPerRating'] = df['regularPrice'] / (df['unitSize'] * df['volume']) / df['consumerRating']
# Store percentage
df['storePercent'] = df['storeCount'] / df['storeCount'].max()

# Drop columns
df = df.drop(columns=['_currentPrice', '_regularPrice', '_source'])
df['time'] = current_time.strftime("%Y-%m-%d %H:%M")

df_periodicdata = df[['time', 'sku', 'currentPrice', 'availableUnits', 'storeCount', 'consumerRating', 'votes']]

# Yearly data
# This is done in feather because csv would be too large :(
if os.path.isfile(f"periodicdata/{current_time.year}-all.csv.bz2"):
    df_read = pd.read_csv(f"periodicdata/{current_time.year}-all.csv.bz2", compression="bz2")
    df_yearly = pd.concat([df_read, df_periodicdata])
else:
    df_yearly = df_periodicdata
df_yearly.to_csv(f"periodicdata/{current_time.year}-all.csv.bz2", compression="bz2")

# Generate "legend" table by SKU
if os.path.isfile(f"periodicdata/{current_time.year}-skus.csv"):
    df_skus = pd.read_csv(f"periodicdata/{current_time.year}-skus.csv")
    # Convert columns
    df_skus['alcoholPercentage'] = df_skus['alcoholPercentage'].apply(lambda cell: cell/100)
    df_skus['regularPrice'] = df_skus['regularPrice'].apply(lambda cell: float(cell))
    # Convert to float
    for col in ['volume']:
        df_skus[col] = df_skus[col].apply(lambda cell: float(cell))
    df_skus.merge(df[['sku', 'alcoholPercentage', 'category_id', 'category', 'certificates', 'class_id', 'class', 'color', 'countryCode', 'countryName', 'grapeType', 'image', 'inventoryCode', 'isBCCraft', 'isBCSpirit', 'isBCVQA', 'isCraft', 'isDealcoholizedWine', 'isDraft', 'isExclusive', 'isKosher', 'isOntarioVQA', 'isOrganic', 'isVQA', 'name', 'namePrefix', 'nameSanitized', 'nameSuffix', 'productCategory', 'productSubCategory', 'productType', 'redVarietal', 'region', 'regularPrice', 'restrictionCode', 'sort', 'style', 'subCategory', 'subCategory_id', 'subRegion', 'sweetness', 'tastingDescription', 'totalAlcoholContent', 'totalVolume', 'unitSize', 'upc', 'volume', 'whiteVarietal']], left_on=['sku'], right_on=['sku'])
else:
    df_skus = df[['sku', 'alcoholPercentage', 'category_id', 'category', 'certificates', 'class_id', 'class', 'color', 'countryCode', 'countryName', 'grapeType', 'image', 'inventoryCode', 'isBCCraft', 'isBCSpirit', 'isBCVQA', 'isCraft', 'isDealcoholizedWine', 'isDraft', 'isExclusive', 'isKosher', 'isOntarioVQA', 'isOrganic', 'isVQA', 'name', 'namePrefix', 'nameSanitized', 'nameSuffix', 'productCategory', 'productSubCategory', 'productType', 'redVarietal', 'region', 'regularPrice', 'restrictionCode', 'sort', 'style', 'subCategory', 'subCategory_id', 'subRegion', 'sweetness', 'tastingDescription', 'totalAlcoholContent', 'totalVolume', 'unitSize', 'upc', 'volume', 'whiteVarietal']]

df_skus.to_csv(f"periodicdata/{current_time.year}-skus.csv")

# Monthly data
if os.path.isfile(f"periodicdata/{current_time.year}-{current_time.month}-all.csv"):
    df_periodicdata.to_csv(f"periodicdata/{current_time.year}-{current_time.month}-all.csv", mode='a', header=False)
else:
    df_periodicdata.to_csv(f"periodicdata/{current_time.year}-{current_time.month}-all.csv")

# Generate "legend" table by SKU
if os.path.isfile(f"periodicdata/{current_time.year}-{current_time.month}-skus.csv"):
    df_skus = pd.read_csv(f"periodicdata/{current_time.year}-{current_time.month}-skus.csv")
    # Convert columns
    df_skus['alcoholPercentage'] = df_skus['alcoholPercentage'].apply(lambda cell: cell/100)
    df_skus['regularPrice'] = df_skus['regularPrice'].apply(lambda cell: float(cell))
    # Convert to float
    for col in ['volume', 'regularPrice', '_currentPrice', '_regularPrice', '_score', 'currentPrice']:
        df_skus[col] = df_skus[col].apply(lambda cell: float(cell))
    df_skus = df_skus.merge(df[['sku', 'alcoholPercentage', 'category_id', 'category', 'certificates', 'class_id', 'class', 'color', 'countryCode', 'countryName', 'grapeType', 'image', 'inventoryCode', 'isBCCraft', 'isBCSpirit', 'isBCVQA', 'isCraft', 'isDealcoholizedWine', 'isDraft', 'isExclusive', 'isKosher', 'isOntarioVQA', 'isOrganic', 'isVQA', 'name', 'namePrefix', 'nameSanitized', 'nameSuffix', 'productCategory', 'productSubCategory', 'productType', 'redVarietal', 'region', 'regularPrice', 'restrictionCode', 'sort', 'style', 'subCategory', 'subCategory_id', 'subRegion', 'sweetness', 'tastingDescription', 'totalAlcoholContent', 'totalVolume', 'unitSize', 'upc', 'volume', 'whiteVarietal']], left_on=['sku'], right_on=['sku'])
else:
    df_skus = df[['sku', 'alcoholPercentage', 'category_id', 'category', 'certificates', 'class_id', 'class', 'color', 'countryCode', 'countryName', 'grapeType', 'image', 'inventoryCode', 'isBCCraft', 'isBCSpirit', 'isBCVQA', 'isCraft', 'isDealcoholizedWine', 'isDraft', 'isExclusive', 'isKosher', 'isOntarioVQA', 'isOrganic', 'isVQA', 'name', 'namePrefix', 'nameSanitized', 'nameSuffix', 'productCategory', 'productSubCategory', 'productType', 'redVarietal', 'region', 'regularPrice', 'restrictionCode', 'sort', 'style', 'subCategory', 'subCategory_id', 'subRegion', 'sweetness', 'tastingDescription', 'totalAlcoholContent', 'totalVolume', 'unitSize', 'upc', 'volume', 'whiteVarietal']]

df_skus.to_csv(f"periodicdata/{current_time.year}-{current_time.month}-skus.csv")
