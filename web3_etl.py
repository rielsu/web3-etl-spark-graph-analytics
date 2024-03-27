from web3 import Web3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame
import csv
import os
from collections import defaultdict

class Web3ETL:
    def __init__(self, api_key, transaction_path, results_path):
        self.api_key = api_key
        self.transaction_path = transaction_path
        self.results_path = results_path
        self.web3 = Web3(Web3.HTTPProvider(f"https://eth-mainnet.alchemyapi.io/v2/{api_key}"))
        self.spark = SparkSession.builder.appName("Web3ETL").getOrCreate()

    def extract_blocks(self, block_file):
        eth_blocks = defaultdict(list)
        try:
            with open(block_file) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for k, v in row.items():
                        eth_blocks[k].append(v)
            return eth_blocks["block_hash"][:100]
        except FileNotFoundError:
            print(f"File {block_file} not found.")
            return []
        except Exception as e:
            print(f"An error occurred while extracting blocks: {e}")
            return []

    def get_transactions_by_block(self, block):
        try:
            block_number = self.web3.eth.get_block(block, full_transactions=True)
            return block_number.transactions
        except Exception as e:
            print(f"An error occurred while retrieving transactions for block {block}: {e}")
            return []

    def save_raw_data_to_parquet(self, transactions, block_hash):
        if not transactions:
            return
        data = [
            (
                index,
                transaction.get("transactionIndex"),
                transaction.get("hash").hex(),
                transaction.get("blockHash").hex(),
                transaction.get("from"),
                transaction.get("to"),
                transaction.get("gas"),
                transaction.get("gasPrice"),
                transaction.get("value"),
            )
            for index, transaction in enumerate(transactions)
        ]
        columns = [
            "transactionOrder",
            "transactionIndex",
            "hash",
            "blockHash",
            "from",
            "to",
            "gas",
            "gasPrice",
            "value",
        ]
        df = self.spark.createDataFrame(data, columns)
        df.write.mode("overwrite").parquet(f"{self.transaction_path}/{block_hash}.parquet")

    def extract_transactions(self, blocks):
        for block in blocks:
            block_path = f"{self.transaction_path}/{block}.parquet"
            if not os.path.exists(block_path):
                transactions = self.get_transactions_by_block(block)
                self.save_raw_data_to_parquet(transactions, block)
            else:
                print(f"Block {block} already extracted.")

    def transform_transactions(self):
        try:
            df = self.spark.read.parquet(f"{self.transaction_path}/*")
            senders = df.select(col("from").alias("id")).distinct()
            receivers = df.select(col("to").alias("id")).distinct()
            vertices = senders.union(receivers)
            edges = df.select(
                col("transactionOrder"),
                col("transactionIndex"),
                col("from").alias("src"),
                col("to").alias("dst"),
                col("hash"),
                col("blockHash"),
                col("gasPrice"),
                (col("gas") * col("gasPrice")).alias("gas_spended"),
                col("value"),
            )
            return GraphFrame(vertices, edges)
        except Exception as e:
            print(f"An error occurred while transforming transactions: {e}")
            return None

    def save_results_to_csv(self, df, filename):
        try:
            df.write.csv(f"{self.results_path}/{filename}.csv", header=True, mode="overwrite")
        except Exception as e:
            print(f"An error occurred while saving results to CSV: {e}")


if __name__ == "__main__":
    etl = Web3ETL(api_key="demo", transaction_path="./transactions", results_path="./results")
    blocks = etl.extract_blocks("eth-blocks_07302021.csv")
    etl.extract_transactions(blocks)
    graph = etl.transform_transactions()
    if graph:
        # Perform graph analysis or save the results to CSV.
        pass
