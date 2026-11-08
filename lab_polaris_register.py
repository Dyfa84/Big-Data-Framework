spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v1.metadata.json'
    )
""").show(truncate=False)

spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
