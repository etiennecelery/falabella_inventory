from bs4 import BeautifulSoup as bs
from csv import writer
from io import StringIO
import json
import math
import pandas as pd
import requests
import sqlite3
import concurrent.futures
import tqdm
import time

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

conn = sqlite3.connect('db.sqlite', check_same_thread=False)
for url in URLS:
    curr_page = 1
    total_pages = 2
    curr_url = url
    while curr_page <= total_pages:
        cat_response = requests.get(curr_url, headers=headers, timeout=60)
        cat_soup = bs(cat_response.content, 'lxml')
        data = cat_soup.find(attrs={'id': '__NEXT_DATA__'}).string
        data_json = json.loads(data)['props']['pageProps']

        if 'results' in data_json.keys():
            df = pd.json_normalize(data_json['results'])
            df['url'] = url
            df.astype(str).to_sql('falabella_products', conn, if_exists='append')

            if curr_page == 1:
                pagination = data_json['pagination']
                total_pages = pagination['count'] / pagination['perPage']
                total_pages = math.modf(total_pages)[1] + 1

        curr_page += 1
        curr_url = url + f"?page={curr_page}"

# URLS = URLS[:4]

# with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
#     future_to_url = {executor.submit(get_data, url): url for url in URLS}
#     for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
#         url = future_to_url[future]
#         future.result()