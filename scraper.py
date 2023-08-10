from bs4 import BeautifulSoup as bs
from csv import writer
from datetime import datetime
from io import StringIO
import json
import math
import pandas as pd
import pickle
import requests
import sqlite3
import concurrent.futures

pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 1000)
pd.set_option('max_colwidth', 800)
pd.options.display.float_format = '{:20,.4f}'.format

conn = sqlite3.connect('db.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)

headers = {
    "referer": "https://www.falabella.com/falabella-cl/product/881557979/Seccional/881557979/?rid=Recs!PDP!FACL!PDP_Carrusel!AB%20Test!Vistos_juntos!881503835!881557979",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",}

url = 'https://www.falabella.com/rest/model/falabella/rest/browse/BrowseActor/get-header-main-menu'
s = requests.Session()
site = s.get(url, headers=headers)
response = site.json()

rootCategories = response['state']['rootCategories']
output = StringIO()
csv_writer = writer(output)

for root in rootCategories:
    root_label = root['label']
    if 'subCategories' in root.keys():
        leafs = root['subCategories']
        for leaf in leafs:
            leaf_label = leaf['label']
            categories = leaf['leafCategories']
            for category in categories:
                category_label = category['label']
                category_link = category['link']
                category_highlight = category['isHighlightLink']
                if category_highlight is False:
                    elements = [root_label, leaf_label,
                                category_label, category_link]
                    csv_writer.writerow(elements)

output.seek(0)
cats = pd.read_csv(output, header=None)
cats.columns = ['rootCategory', 'leafCategory', 'category', 'url']

beginning_url = 'https://www.falabella.com/falabella-cl'
cats['url'] = beginning_url + cats['url']
URLS = cats["url"].tolist()


def get_data(url, timeout=60):
    curr_page = 1
    curr_url = url
    total_pages = 2
    while curr_page <= total_pages:
        cat_response = requests.get(curr_url, headers=headers, timeout=timeout)
        cat_soup = bs(cat_response.content, 'lxml')
        data = cat_soup.find(attrs={'id': '__NEXT_DATA__'}).string
        data_json = json.loads(data)['props']['pageProps']

        if 'results' in data_json.keys():
            df = pd.json_normalize(data_json['results'])
            df['url'] = url

            if curr_page == 1:
                pagination = data_json['pagination']
                total_pages = pagination['count'] / pagination['perPage']
                total_pages = math.modf(total_pages)[1] + 1

        curr_page += 1
        curr_url = url + f"?page={curr_page}"

    return True


container = list()
errors = list()
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    future_to_url = {executor.submit(get_data, url, 60): url for url in URLS}
    for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
        url = future_to_url[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %.1f' % (url, exc))
            errors.append([url, exc])
        else:
            print('%r has %d items, %r' % (url, len(data), 100*i/len(URLS)))
            container.append(data)

df = pd.concat(container, ignore_index=True)

df = df.loc[df.productId.drop_duplicates().index].copy()

prices = df['prices'].apply(pd.Series)

prices[0] = prices[0].apply(lambda x: x['price'][0])
prices[1] = prices[1].apply(lambda x: x['price'][0] if type(x) == dict else x)
replace_map = {0: 'precio_oferta', 1: 'precio_normal'}
prices = prices[[0, 1]].rename(columns=replace_map)

prices = prices.apply(lambda x: x.str.replace('.', '', regex=False))
prices = prices.apply(pd.to_numeric)

df = pd.concat([df, prices], axis=1, sort=False)
df['precio_normal'] = df['precio_normal'].fillna(df['precio_oferta'])
today = datetime.today().date()
df['date'] = today

df = df.reset_index(drop=True)

df['variants'] = df['variants'].apply(lambda x: x[0])
df = df.eval('discount = 1 - precio_oferta / precio_normal')
df = df.merge(cats, on='url')


if len(errors) > 0:
    print(errors)
    with open('errors.pkl', 'wb') as file:
        pickle.dump(errors, file)


BEGINNING_URL = 'https://www.falabella.com/falabella-cl'
ADD_ITEM_URL = 'https://www.falabella.com/rest/model/atg/commerce/order/purchase/CartModifierActor/addItemToBasket'


def get_stock(sku, product_id, product_url, timeout=60):
    result = None
    s = requests.Session()
    s.get(BEGINNING_URL, headers=headers, timeout=timeout)

    basket_headers = {
        "referer": product_url,
        "origin": "https://www.falabella.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "content-type": "application/json",
        "accept": "*/*",
        "content-length": "101",
        "accept-language": "es-ES,es;q=0.9,en;q=0.8",
    }

    payload = {
        'formSubmissionData':
            [
                {'skuId': sku, 'productId': product_id,
                 'hasVariations': False, 'quantity': 500}
            ]
    }

    addItem = s.post(ADD_ITEM_URL, headers=basket_headers,
                     json=payload, allow_redirects=True, timeout=timeout)

    addItem_response = json.loads(addItem.content)

    if len(addItem_response['errors']) > 0:
        error = addItem_response['errors'][0]['message']
        if 'Sólo quedan' in error:
            result = int(error.split('Sólo quedan ')[1].split(' ')[0])

    return [product_id, result]

container = list()
inventory_errors = list()
with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    future_to_url = {executor.submit(get_stock, r.skuId, r.productId, r.url):
                     r.url for i, r in df.iterrows()}
    for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
        url = future_to_url[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
            inventory_errors.append([url, exc])
        else:
            print('%s has %s' % (url, data[1]))
            container.append(data)

inventory = pd.DataFrame(data=container, columns=['productId', 'stock'])
df = df.merge(inventory, on='productId')

df.to_csv(f'base_{pd.Timestamp.today().strftime("%Y-%m-%d")}.csv')
