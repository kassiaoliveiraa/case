import functions_framework
import requests
from collections import defaultdict
from datetime import datetime
import pandas as pd
from google.cloud import storage

BUCKET_NAME = "case-enjoei"

@functions_framework.http
def generate_user_cart_info(request):
    base_url = "https://fakestoreapi.com"

    carts = requests.get(f"{base_url}/carts")
    products = requests.get(f"{base_url}/products")

    if carts.status_code == 200 and products.status_code == 200:
        carts = carts.json()
        products = products.json()

        product_categories = {product['id']: product['category'] for product in products}

        user_info = {}

        for cart in carts:
            user_id = cart['userId']
            date = cart['date']

            if user_id not in user_info:
                user_info[user_id] = {
                    "last_add_date": date,
                    "category_counts": defaultdict(int)
                }

            if date > user_info[user_id]["last_add_date"]:
                user_info[user_id]["last_add_date"] = date

            for item in cart['products']:
                product_id = item['productId']
                category = product_categories.get(product_id)
                if category:
                    user_info[user_id]["category_counts"][category] += 1

        dataset = []
        for user_id, data in user_info.items():
            top_category = max(data["category_counts"], key=data["category_counts"].get)
            dataset.append({
                "user_id": user_id,
                "last_add_date": data["last_add_date"],
                "top_category": top_category,
                "data_process": datetime.now().__format__("%Y-%m-%d %H:%M:%S")
            })

        df = pd.DataFrame(dataset)
        date_time = f"{datetime.now():%Y%m%d%H%M%S}"
        filename = f"infocarts_{date_time}.parquet"

        tem_file = f"/tmp/{filename}"
        df.to_parquet(tem_file, engine='pyarrow', compression='gzip')

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_filename(tem_file)

        return "Arquivo Parquet gerado e salvo com sucesso no bucket do Cloud Storage.", 200
    else:
        return "Erro ao acessar as APIs.", 500