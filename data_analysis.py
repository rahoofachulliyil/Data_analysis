import pandas as pd
import mysql.connector
import json
file_name='data.json'
with open(file_name, 'r') as json_file:
    Data = json.load(json_file)


products = Data['items']


names = []
prices = []
stocks = []
quantities = []
product_urls = []
image_urls = []

for product in products:
    # print (f"product is:{product}")
    name = product['name']
    price = product['uom_price']['price']
    stock_level = product['availability']['stock_level']
    quantity = product['base_quantity']
    product_url = f"https://sprouts.com{product['href']}"
    image_url = product['images']['tile']['medium']
    
    
    stock = stock_level > 0
    
  
    names.append(name)
    prices.append(price)
    stocks.append(stock)
    quantities.append(quantity)
    product_urls.append(product_url)
    image_urls.append(image_url)

df = pd.DataFrame({
    'name': names,
    'price': prices,
    'stock': stocks,
    'quantity': quantities,
    'product_url': product_urls,
    'image_url': image_urls
})
# print(df.to_string)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="rahoofa@123",
    database="sakila"
)


cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS Products (
                    name VARCHAR(255),
                    price INT,
                    stock BOOLEAN,
                    quantity INT,
                    product_url VARCHAR(255),
                    image_url VARCHAR(255)
                )''')


for index, row in df.iterrows():
    print(f"{index+1}th row is:{row}")

    cursor.execute('''INSERT INTO Products (name, price, stock, quantity, product_url, image_url)
                      VALUES (%s, %s, %s, %s, %s, %s)''', (row['name'], row['price'], row['stock'], row['quantity'], row['product_url'], row['image_url']))


conn.commit()
conn.close()
