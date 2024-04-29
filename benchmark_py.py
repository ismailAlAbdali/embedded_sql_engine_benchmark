import sys
import os
import psutil
import time
import pandas as pd # pandas for plotting the results
import matplotlib.pyplot as plt
# importing embedded sql engines
import chdb
import duckdb
import datafusion
import glaredb
 


#  to do
# 1. ensure that the metrics and collected for all of the queries in all eninges
# 2. make the query files for chDB sql engine and ensure the query string work smootly
# 3. make the plotting more in one funcution
# 4. 




# CONSTANTS
SQL_ENGINES = ["datafusion", "duckdb", "glaredb"] # the SQL engines
QUERIES_NUM = 4 # the numebr of quries we are going to benchmark
MAX_INTERATIONS = 3 # interate over running the quuries on different databases 10 time
DIRECTORY = "./csv_files"

def load_query(db, query_num):
    try:
        with open(f"./queries/{db}.query_{query_num}.sql") as file:
            return file.read()
    except FileNotFoundError:
        print(f"Query file for {db}.query_{query_num}.sql not found.")
        sys.exit(1)


def get_csv_paths_and_table_names(directory):
    """
    Get the CSV files paths and the table names to regiester within some sql engines such as datafusion. 
    """
    dir_files = os.listdir(directory)
    csv_files_path = []
    table_names = []

    # Filter and append only CSV files to the list
    for file_name in dir_files:
        if file_name.endswith('.csv'):
            full_path = os.path.join(directory, file_name)
            csv_files_path.append(full_path)
            table_names.append(file_name.replace(".csv",""))
    
    return csv_files_path,table_names



def run_query_and_monitor(engine, query_exec_func):
    """ Run the query and monitor the process, returning execution metrics. """
    pid = psutil.Process().pid
    start_time = time.time()
    result = query_exec_func()
    end_time = time.time()
    execution_time = end_time - start_time
    process = psutil.Process(pid)
    cpu_usage = process.cpu_percent(interval=execution_time)  # Get the average CPU usage during the execution
    memory_usage = process.memory_info().rss / (1024 ** 2)  # Convert memory usage to MB
    return {
        "engine": engine,
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "execution_time": execution_time
    }





csv_files_path,table_names = get_csv_paths_and_table_names(DIRECTORY)





execution_metrics = []
for engine in SQL_ENGINES:
    for query_num in range(QUERIES_NUM):
        query = load_query(engine, query_num + 1) # get the query
        for itr in range(MAX_INTERATIONS):
            match engine:
                case "duckdb":
                    print(f"Testing {engine}")
                    con = duckdb.connect()
                    metrics = run_query_and_monitor(engine, lambda: con.execute(query))
                    con.close()
                    
                case "glaredb":
                    print(f"Testing {engine}")
                    gdb = glaredb.connect()
                    metrics = run_query_and_monitor(engine, lambda: gdb.sql(query))
                    gdb.close()
    
                case "datafusion":
                    print(f"Testing {engine}")
                    ctx = datafusion.SessionContext()
                    # Register tables and calculate registration time
                    registration_times = []
                    for k in range(len(table_names)):
                        reg_start_time = time.time()
                        ctx.register_csv(table_names[k], csv_files_path[k])
                        reg_elapsed_time = time.time() - reg_start_time
                        registration_times.append(reg_elapsed_time)
                    metrics = run_query_and_monitor(engine, lambda: ctx.sql(query).collect())
                    metrics['execution_time'] = metrics['execution_time'] + sum(registration_times)  # Add registration time to metrics, include disk I/O time
            metrics["query"] = f"query {query_num + 1}"
            execution_metrics.append(metrics)

# Convert collected data into a DataFrame
df_metrics = pd.DataFrame(execution_metrics)



# Group by query and engine, computing averages
df_summary = df_metrics.groupby(['query', 'engine']).agg({
    'cpu_usage': 'mean',
    'memory_usage': 'mean',
    'execution_time': 'mean'
}).reset_index()

# Separate into different DataFrames for each metric to simplify plotting
cpu_summary = df_summary.pivot(index='query', columns='engine', values='cpu_usage')
memory_summary = df_summary.pivot(index='query', columns='engine', values='memory_usage')
time_summary = df_summary.pivot(index='query', columns='engine', values='execution_time')

# Plotting functions
def plot_bar(df, title, ylabel,y_min = 0.0001, y_max = 1,log_scale = False):
    plt.figure(figsize=(12, 6))
    df.plot(kind='bar')
    plt.title(title)
    plt.xlabel('Query')
    plt.ylabel(ylabel)
    plt.yscale('log')  # Set the y-axis to a logarithmic scale
    plt.ylim(y_min, y_max)  # Set the range for the y-axis from 0.0001 to 1
    plt.legend(title='Engine')
    plt.tight_layout()
    plt.show()

# Plot each metric
plot_bar(cpu_summary, 'Average CPU Utilization per Query', 'Average CPU Utilization (%)')
plot_bar(memory_summary, 'Average Memory Usage per Query', 'Average Memory Usage (MB)',y_min = 0,y_max = 1000)
plot_bar(time_summary, 'Average Execution Time per Query', 'Average Execution Time (s)')
