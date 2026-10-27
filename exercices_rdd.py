import re
from operator import add

# Run inside the pyspark shell: `sc` is the SparkContext.
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")

header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)

def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)

user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)

def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)

def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)

# Exercise 1
user_stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),
        lambda acc, q: (acc[0] + 1, acc[1] + q),
        lambda a, b: (a[0] + b[0], a[1] + b[1]),
    )
)
user_avg = user_stats.mapValues(lambda s: s[1] / s[0])
