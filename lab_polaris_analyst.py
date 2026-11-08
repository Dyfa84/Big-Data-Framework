spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
for stmt in [
    "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1",
    "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING",
]:
    try:
        spark.sql(stmt)
        print("UNEXPECTED SUCCESS:", stmt)
    except Exception as e:
        print("DENIED:", stmt)
        print(str(e)[:300])
