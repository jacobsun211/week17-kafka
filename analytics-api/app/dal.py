from connection import connection







def customers_with_most_orders():
    sql = """
    SELECT customerNumber ,COUNT(orderNumber)
    FROM orders 
    GROUP BY COUNT(orderNumber)
    ORDER BY COUNT(orderNumber) DESC
    LIMIT(10)
        """
    cursor = connection.cursor()
    cursor.execute(sql)
    table = cursor.fetchall()
    cursor.close()
    connection.close()

    query_result = []
    for column in table:
        query_result.append(
            {
                "customerNumber": column[0],
                "num_of_orders": column[1],
            }
        )

    return query_result