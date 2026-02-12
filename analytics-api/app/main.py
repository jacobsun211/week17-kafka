from fastapi import FastAPI
import dal

app = FastAPI()




@app.get("/analytics/top-customers")
def customersmost_orders():
    return dal.customers_with_most_orders()