import requests
from collections import defaultdict
from datetime import datetime
import pandas as pd

baseUrl = "https://fakestoreapi.com"
carts = requests.get(baseUrl+"/carts")
products = requests.get(baseUrl+"/products")

if carts.status_code == 200 and products.status_code == 200 :
    carts = carts.json()
    products = products.json()

    product_categories = {product['id']: product['category'] for product in products}

    userInfo = {}

    for cart in carts:

        user_id = cart['userId']
        date = cart['date']

        if user_id not in userInfo:
            userInfo[user_id] = {
                "last_add_date": date,
                "category_counts": defaultdict(int)
            }

        if date > userInfo[user_id]["last_add_date"]:
            userInfo[user_id]["last_add_date"] = date

        for item in cart['products']:
            product_id = item['productId']
            category = product_categories.get(product_id)
            if category:
                userInfo[user_id]["category_counts"][category] += 1

    dataset = []

    for user_id, data in userInfo.items():
        top_category = max(data["category_counts"], key=data["category_counts"].get)
        dataset.append({
            "user_id": user_id,
            "last_add_date": data["last_add_date"],
            "top_category": top_category,
            "data_process": datetime.now().__format__("%Y-%m-%d %H:%M:%S")
        })

    df = pd.DataFrame(dataset)
    data_time = f"{datetime.now():%Y%m%d%H%M%S}"

    path = f'.//infocarts_{data_time}.parquet'

    df.to_parquet(path, engine='pyarrow', compression='gzip')
    print("Arquivo parquet gerado com sucesso")
    print(df)

else:
    print("Erro ao acessar as APIs.")