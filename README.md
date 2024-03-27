# Web3 Transactions ETL and Analysis with GraphFrames

This project is a demonstration of an ETL (Extract, Transform, Load) pipeline for Ethereum transactions using the Web3 library, PySpark, and GraphFrames. The pipeline extracts transaction data from Ethereum blocks, transforms it into a format suitable for analysis, and performs various analytics on the data, including finding the most expensive transactions, most frequent addresses, largest transactions per address, and more.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.6 or higher
- PySpark
- Web3
- GraphFrames

You can install the required Python packages using:

```bash
pip install pyspark web3 graphframes
```

## Installation

### Clone the repository:

```bash
git clone https://github.com/your-username/web3-transactions-etl.git

```
### Navigate to the project directory:

```bash
cd web3-transactions-etl

```
### Install the required Python packages:

```bash
pip install -r requirements.txt

```

## Usage

To run the ETL pipeline and perform the analysis, execute the main script:

```bash
python get_insights.py

```

