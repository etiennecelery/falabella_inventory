import ast
import concurrent.futures
from csv import writer
from datetime import datetime
from io import StringIO
import glob
import json
import numpy as np
import pandas as pd
import requests

pd.set_option('display.max_columns',200)
pd.set_option('display.max_rows',1000)
pd.set_option('max_colwidth', 800)
pd.options.display.float_format = '{:20,.4f}'.format

headers = {
    "referer": "https://www.falabella.com/falabella-cl/product/881557979/Seccional/881557979/?rid=Recs!PDP!FACL!PDP_Carrusel!AB%20Test!Vistos_juntos!881503835!881557979",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",}

filename = glob.glob('bases/*')[-1]
df = pd.read_csv(filename, low_memory=False)
df = df.loc[df['skuId'].drop_duplicates().index]
print(len(df))
errors = []
print(datetime.today())

def get_stock(sku_list, q):
    print(len(sku_list))
    addItemUrl = 'https://www.falabella.com/rest/model/atg/commerce/order/purchase/CartModifierActor/addItemToBasket'
    beginning_url = 'https://www.falabella.com/falabella-cl'

    s = requests.Session()
    s.get(beginning_url, headers=headers)

    output = StringIO()
    csv_writer = writer(output)

    for i, item_id in enumerate(sku_list):
        print(i)
        try:
            productId = df[df['skuId']==item_id]['productId'].iloc[0]
            item_url = df[df['skuId']==item_id]['url'].iloc[0]
            options = ast.literal_eval(df[df['skuId']==item_id]['variants'].iloc[0])['options']
            if len(options) > 0:
                item_ids = [x['mediaId'] for x in options]
            else:
                item_ids = [item_id]

            for item_id in item_ids:

                basket_headers = {
                    "referer": item_url,
                    "origin": "https://www.falabella.com",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "content-type": "application/json",
                    "accept": "*/*",
                    "content-length": "101",
                    "accept-language": "es-ES,es;q=0.9,en;q=0.8",
                }

                payload = '{"formSubmissionData":[{"skuId":"%s","productId":"%s","hasVariations":false,"quantity":500}]}'%(item_id, productId)

                addItem = s.post(addItemUrl, headers=basket_headers, data=payload, allow_redirects=True)
                addItem_response = json.loads(addItem.content)
                if len(addItem_response['errors']) > 0:
                    error = addItem_response['errors'][0]['message']
                    if 'Sólo quedan' in error:
                        error = error.split('Sólo quedan ')[1].split(' ')[0]
                    else:
                        error = np.nan
                else:
                    error = 500

                csv_writer.writerow([productId, item_id, error])
        except:
            errors.append(item_id)
            pass
    try:
        output.seek(0)
        inventory = pd.read_csv(output, header=None)
        inventory.columns = ['productId','skuId','units']
        q.put(inventory)

    except:
        print('Error!')
        q.put(pd.DataFrame())

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

list_to_chunk = df['skuId'].drop_duplicates().tolist()
chunk_list = chunks(list_to_chunk, int(len(list_to_chunk)/20))

queues, threads = [], []
main_list = df['skuId'].drop_duplicates().tolist()
for chunk in chunk_list:
    q = queue.Queue()
    threads.append(threading.Thread(target=get_stock, args=(chunk, q)))
    queues.append(q)

for thread in threads:
    thread.start()
container = []
for q in queues:
    container.append(q.get())
for thread in threads:
    thread.join()
print('threads joined')

inventory = pd.concat(container, axis=0, sort=False, ignore_index=True)
inventory['skuId'] = inventory['skuId'].astype('str')
inventory = inventory.loc[inventory['skuId'].drop_duplicates().index]
inventory.to_csv(f'inventory {datetime.today().strftime("%Y-%m-%d")}.csv', index=False)
