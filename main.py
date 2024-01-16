import pandas as pd
import gzip
import json
from bs4 import BeautifulSoup
import numpy as np


def parse(path):
    g = gzip.open('meta_Movies_and_TV.json.gz', 'r')
    for l in g:
        yield json.loads(l)


def prase(path):
    g = gzip.open('meta_Movies_and_TV.json.gz', 'rb')
    for l in g:
        yield json.loads(l)


def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')


df = getDF('meta_Movies_and_TV.json.gz')
url = ['custommeet3.centralus.cloudapp.azure.com']
df['url'] = pd.np.tile(url, len(df) // len(url)).tolist() + url[:len(df) % len(url)]
status = ['A']
df['status'] = pd.np.tile(status, len(df) // len(status)).tolist() + status[:len(df) % len(status)]
condition = ['New']
df['condition'] = pd.np.tile(condition, len(df) // len(status)).tolist() + condition[:len(df) % len(condition)]
vendorId = [29]
df['vendorId'] = pd.np.tile(vendorId, len(df) // len(vendorId)).tolist() + vendorId[:len(df) % len(vendorId)]
manufacturer = df['brand']
df['manufacturer'] = manufacturer

df['category'] = df["category"].str[0]
df['category'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['category']])
df['category'].replace('nan', np.nan, inplace=True)
df['category'] = df['category'].dropna()
df['feature'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['feature']])
df['description'] = pd.DataFrame([str(line).strip('[').strip(']').strip("'").strip('"')for line in df['description']])

df1=df['description'].tolist()
n=len(df['description'])
for i in range(0,n):
  if not df['description'][i]:
      soap = df['description'][i]
  else:
      text = str(df['description'][i])
      soap = BeautifulSoup(text, 'html.parser').text
      df['description'][i] = soap

df['price'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['price']])
n=len(df['price'])
for i in range(0,n):
    if (len(str(df['price']))>=7):
        df['price']=np.nan
    elif not df['price'][i]:
        rawPrice = df['price'][i]
    else:
        testeee = df['price']
        rawPrice = str(df['price'][i])
        print('ZZZZZZZZZZZZZZZZZZZZZZZ' + rawPrice)
        textPrice = BeautifulSoup(rawPrice, 'html.parser').text
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXX' + textPrice)
        df['price'][i] = textPrice

df['asin'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['asin']])
df['brand'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['brand']])
df['imageURL'] = df["imageURL"].str[0]
df['imageURL'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['imageURL']])
df['imageURL'].replace('nan', np.nan, inplace=True)
df['title'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df['title']])
df = df[["title", "imageURL", "brand", "manufacturer", "asin", "price", "description", "feature", "category", "url",
         "status", "condition", "vendorId"]]
print(len(df))

df.rename(columns={'title': 'name', 'imageURL': 'imgUrl', 'asin': 'sku', 'feature': 'keywords'}, inplace=True)
newdf = df.replace(r'^\s*$', np.nan, regex=True)
df = newdf.dropna()
df.to_excel('m.xlsx', index=False, header=True)
