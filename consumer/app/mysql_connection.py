import os, pymysql



MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MYSQL_CFG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": "root",
    "password": "password",
    "database": "analytics_db",
    "autocommit": True
}

async def create_tables():
    conn = pymysql.connect(**MYSQL_CFG)
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS customers (
                    customerNumber INT NUT NULL PRIMARY KEY,
                    customerName VARCHAR(50),
                    contactLastName VARCHAR(50),
                    contactFirstName VARCHAR(50),
                    phone INT
                    addressLine1 VARCHAR(50)
                    addressLine2 VARCHAR(50)
                    city VARCHAR(50)
                    state VARCHAR(50)
                    postalCode VARCHAR(50)
                    country VARCHAR(10)
                    salesRepEmployeeNumber INT
                    creditlimit VARCHAR(50)
                    )""")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS orders (
                    orderNumber INT NUT NULL PRIMARY KEY,
                    orderDate VARCHAR(50),
                    requiredDate VARCHAR(50),
                    shippedDate VARCHAR(50),
                    status VARCHAR(50)
                    comments VARCHAR(255)
                    customerNumber INT 
                    )""")
        print("created tabels")
        return conn
    # FOREIGN KEY REFERENCES customers(customerNumber)
    

async def insert_to_sql(document: dict):
    conn = create_tables()
    conn = pymysql.connect(**MYSQL_CFG)

    with conn.cursor() as cur:
        if document["type"] == "customer":
            sql = """INSERT IGNORE INTO customers (
                    customerNumber, customerName, contactLastName, contactFirstName,phone,addressLine1,addressLine2,city,state,postalCode,country,salesRepEmployeeNumber,creditlimit
                    ) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s)"""
            
            data = []
           
            customerNumber = document["customerNumber"]
            customerName = document["customerName"]
            contactLastName = document["contactLastName"]
            contactFirstName = document["contactFirstName"]
            phone = document["phone"]
            addressLine1 = document["addressLine1"]
            addressLine2 = document["addressLine2"]
            city = document["city"]
            state = document["state"]
            postalCode = document["postalCode"]
            country = document["country"]
            salesRepEmployeeNumber = document["salesRepEmployeeNumber"]
            creditlimit = document["creditlimit"]
            data.append((customerNumber, customerName, contactLastName, contactFirstName,phone, addressLine1, addressLine2, city,state, postalCode, country, salesRepEmployeeNumber,creditlimit))

            cur.execute(sql, data)

        elif document["type"] == "order":
            sql = """INSERT IGNORE INTO customers (
                    orderNumber, orderDate, requiredDate, shippedDate,status,comments,customerNumber
                    ) VALUES (%s, %s, %s, %s,%s, %s, %s)"""

            data = []

            orderNumber = document["orderNumber"]
            orderDate = document["orderDate"]
            requiredDate = document["requiredDate"]
            shippedDate = document["shippedDate"]
            status = document["status"]
            comments = document["comments"]
            

            data.append((orderNumber, orderDate, requiredDate, shippedDate,status, comments))

            cur.execute(sql, data)

        print("inserted")


            
        conn.close()
        return

 




