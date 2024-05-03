# Embedded SQL Engine Benchmark

## Overview

This project aims to benchmark the performance of four different databases: DataFusion, CHDB, DuckDB, and Glare DB. We evaluate these databases on various metrics such as CPU utilization, memory utilization, and query latency. The evaluation is conducted using four specifically tailored queries

## Project Structure
### Directories

- 'queries/': Contains SQL query files for each database. Each file is named in the format
  - ___'<database>.query\_\<query number>.sql'___
- 'csv_files/': Contains CSV data files used as input for the queries.

### Data Files

- ___'dimcustomer.csv'___: Contains details about customers.
- ___'dimdate.csv'___: Includes information regarding dates.
- ___'dimproduct.csv'___: Stores product information.
- ___'factsales.csv'___: Stores sales data.

## Getting Started
### Prerequisites
To run the benchmarks, you need Python installed on your machine, other required Python libraries can be installed using the ___'requirements.txt'___ file included in the repository.

- _pip install -r requirements.txt_

### Running the Benchmarks
- Navigate to the project directory.
- Execute the benchmark script.
  - _python benchmark_py.py_

