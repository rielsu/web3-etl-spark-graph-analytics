from pyspark.sql.functions import col, rank, row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from web3_etl import Web3ETL  # Assuming the class is saved in web3_etl.py

# Initialize the Web3ETL object
etl = Web3ETL(api_key="demo", transaction_path="./transactions", results_path="./results")

# Extract and transform transactions
blocks = etl.extract_blocks("eth-blocks_07302021.csv")
etl.extract_transactions(blocks)
g = etl.transform_transactions()

# Analyze the transactions
edges = g.edges

# Most expensive transactions
most_expensive_transactions = edges.orderBy(col("gas_spended").desc())
etl.save_results_to_csv(most_expensive_transactions.limit(20), "most_expensive_transactions")

# Most frequent addresses
most_frequent_addresses = g.degrees.orderBy(col("degree").desc())
etl.save_results_to_csv(most_frequent_addresses.limit(20), "most_frequent_addresses")

# Largest transaction per most frequent address
jd = most_frequent_addresses.join(edges, most_frequent_addresses.id == edges.src, "inner")
window = Window.partitionBy("id").orderBy("value")
jd = jd.withColumn("rank", rank().over(window))
largest_transaction_most_frequent_address = jd.select(col("id"), col("blockHash"), col("hash"), col("value")).where((col("rank") == 1) & (col("value").isNotNull())).orderBy(col("value").desc())
etl.save_results_to_csv(largest_transaction_most_frequent_address.limit(100), "largest_transaction_most_frequent_address")

# Largest receivers
largest_receivers = g.inDegrees.orderBy(col("inDegree").desc())
etl.save_results_to_csv(largest_receivers.limit(100), "largest_receivers")

# Largest senders
largest_senders = g.outDegrees.orderBy(col("outDegree").desc())
etl.save_results_to_csv(largest_senders.limit(100), "largest_senders")

# Strongly connected addresses
strongly_connected_addresses = g.stronglyConnectedComponents(maxIter=10).orderBy(col("component").desc())
etl.save_results_to_csv(strongly_connected_addresses.limit(100), "strongly_connected_addresses")

# Addresses communities
addresses_communities = g.labelPropagation(maxIter=5)
etl.save_results_to_csv(addresses_communities.limit(100), "addresses_communities")

# Largest communities
largest_communities = addresses_communities.groupBy("label").count().orderBy(col("count").desc())
etl.save_results_to_csv(largest_communities.limit(100), "largest_communities")

# Transaction reordering
window = Window.partitionBy("blockHash").orderBy("gasPrice")
jd = edges.withColumn("row_number", row_number().over(window))
transaction_reordering = jd.where(jd.transactionOrder == jd.row_number - 1).groupBy("blockHash").agg(F.count("hash").alias("risky_transaction_count")).orderBy(col("risky_transaction_count").desc())
etl.save_results_to_csv(transaction_reordering.limit(100), "transaction_reordering")
