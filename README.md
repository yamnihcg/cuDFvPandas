# An Automated Benchmarking Tool for cuDF vs. Pandas
This guide will explain how to run this benchmarking tool for DataFrame operations in cuDF vs. pandas. The user has the option to run this tool through a Command Line Interface (CLI) or a Jupyter Notebook. At the end of this guide, the reader will be able to view benchmarking times, speedups, and various performance visualizations for pandas and cuDF operations. 

## Context / Introduction
What do self-driving cars, recommender systems, and spam filters have in common? The answer is that they are all powered by Artificial Intelligence. In recent years, data scientists and engineers have been tasked with “cleaning up” large datasets so that they can feed this data into Artificial Intelligence models. One of the most popular technologies to “clean up” datasets today is pandas, a CPU based DataFrame library. However, one huge drawback of pandas is that it has poor performance on large datasets. 

In response to this clear need, NVIDIA released cuDF, a GPU-based DataFrame library. Since cuDF and pandas have identical syntax (refer to graphic below), it is very easy for data scientists and engineers to adapt to this new library. In order to demonstrate cuDF’s computational advantage, we have developed a tool that automatically benchmarks the time(s) that cuDF and pandas take for common DataFrame operations. The data we are using for this tool is from the New York City taxi dataset, divided into month by month chunks. This is done in 2 formats, a Command Line Interface (CLI) and a Jupyter Notebook. 

## Prerequisites

To proceed to the next steps, these instructions that you have the following already installed: <br />


**GPU:** NVIDIA Pascal™ or better with compute capability 6.0+ <br /> 
**CUDA:** Version 10.0+ (with NVIDIA drivers) <br /> 
**OS:** Ubuntu 16.04/18.04 <br /> 
**Docker:** Docker CE v19.03+ and nvidia-container-toolkit (if using docker container) <br /> 

## Start RAPIDS Docker Container 

1. Go to https://rapids.ai/start.html and scroll down to the RAPIDS release selector <br />
**Method:** select Docker + Examples <br />
**Release:** select Stable Release <br />
**Packages:** select All Packages or cuDF <br />
**Linux:** Run lsb_release -a to find out your computer or VM’s Ubuntu version. <br />
**Python / CUDA:** Select the versions on your computer or VM <br />

2. Copy the command generated from the RAPIDS release selector into the terminal in order to build your Docker container (refer to image below)

3. Once in the docker container, navigate to the outermost directory (using cd ..) and then run bash /rapids/utils/start_jupyter.sh (refer to image below)

![Docker Command](https://github.com/yamnihcg/cuDFvPandas/blob/master/docker_container_example_run.PNG)

4. Visit localhost:8888 or (ip address of your VM):8888 in your browser. An instance of JupyterLab should show up.

5. Click Terminal (under other). In the Terminal window, install the gcsfs python package using pip install gcsfs. If other packages used in the tool are missing as its run, use pip install (packagename) to install those packages (this is highly unlikely though). 

## Running the Tool - Command Line (CLI) 

1. From this repository, download the files cudf_benchmarking_v2.py, load_data_v2.py, and rcid_to_meaning.csv. On the left hand side of the JupyterLab window, there is a menu of files. Drag them into the same folder / directory in JupyterLab.

2. Open a Terminal window in JupyterLab by navigating to the menu bar at the top and selecting File → New → Terminal.

3. Before running the benchmarking tool, two parameters are required. These are the number of months of data you want to look at and the operation that you want to benchmark against. The number of months of data can be anywhere from 1 month to 36 months. The operations are represented in shorthand notation below:

| Notation | Description |
|------|------|
| af | Apply numerical function on a column  |
| aggfunc  | Apply aggregate function on a column |
| gb | Group by category + Apply Aggregate function |
| gt5 | Sort rows based on column value |
| dd  | Drop duplicate rows |
| dc | Drop column from DataFrame  |
| fc | Filter rows from DataFrame ( by column value ) |
| ld | Load data into DataFrame |
| rtc  | Rewrite DataFrame to .csv file |
| cdf | Concatenate DataFrames together |
| fnv  | Fill null (missing) values in DataFrame |
| merge | Perform an inner join on two DataFrames |
| all | Perform all operations above (w/ the exception of cdf) |

4. Run the following command in your terminal. (months) and (operation) are the months and operation you decided on in the previous step. 
python cudf_benchmarking_v2.py -num_months (months) -operation (operation)

5. After running the command, the results show up on the command line. If you entered ‘all’ for the operation, visualizations are also generated. The image filenames of the visualizations as well as what each visualization contains is below: 

| Filename | Visualization |
|------|------|
| cudf_vs_pandas_p1.png | This visualization is a double bar chart that compares the time it takes for less intensive DataFrame operations to finish in pandas versus cuDF. |
| cudf_vs_pandas_p2.png | This visualization is a double bar chart that compares the time it takes for more intensive DataFrame operations to finish in pandas versus cuDF. |
| speedups.png | This visualization is a bar chart that looks at how many times faster cuDF is (as compared to pandas) for a given operation. |

## Running the Tool - Jupyter Notebook

1. From this repository, download the notebook cudf_vs_pandas.ipynb. Drag the file into the files menu (on the left side of the interface) in JupyterLab.

2. Open up the file by double clicking on the file name. Run each cell by pressing the ▷ button in the menu inside the notebook. Do this until the “Loading the Data for Benchmarking” section. This is necessary because running each cell ensures that the functions that are used for benchmarking are in the program’s memory. 

3. In this cell (image below) enter the number of months of data and the operation you want to benchmark against. Refer to the Running the Tool - Command Line (CLI) section to double check that your inputs are valid.

![Enter Benchmarking Info](https://github.com/yamnihcg/cuDFvPandas/blob/master/enter_benchmarking_info.PNG)

## Example Output - 1 Month of Data 

As an example, lets run the benchmarking tool on 1 Month of taxi data (~2 GB) and look at how all of the operations perform. 

**CLI** <br />
Using the CLI, the command would look like: <br />

```
python cudf_benchmarking_v2.py -num_months 1 -operation all
```

**Jupyter Notebook** <br />
In the Jupyter Notebook, set num_months = 1 and operation = 'all' in the appropriate cell (refer to Step 3 in Running the Tool - Jupyter Notebook)










