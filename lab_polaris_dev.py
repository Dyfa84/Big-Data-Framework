# Create tables as dev
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)

stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()

# Schema change as dev
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show()
