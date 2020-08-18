# An Automated Benchmarking Tool for cuDF vs. Pandas
This guide will explain how to run this benchmarking tool for DataFrame operations in cuDF vs. pandas. The user has the option to run this tool through a Command Line Interface (CLI) or a Jupyter Notebook. At the end of this guide, the reader will be able to view benchmarking times, speedups, and various performance visualizations for pandas and cuDF operations. 

## Context / Introduction
What do self-driving cars, recommender systems, and spam filters have in common? The answer is that they are all powered by Artificial Intelligence. In recent years, data scientists and engineers have been tasked with “cleaning up” large datasets so that they can feed this data into Artificial Intelligence models. One of the most popular technologies to “clean up” datasets today is pandas, a CPU based DataFrame library. However, one huge drawback of pandas is that it has poor performance on large datasets. 

In response to this clear need, NVIDIA released cuDF, a GPU-based DataFrame library. Since cuDF and pandas have identical syntax (refer to graphic below), it is very easy for data scientists and engineers to adapt to this new library. In order to demonstrate cuDF’s computational advantage, we have developed a tool that automatically benchmarks the time(s) that cuDF and pandas take for common DataFrame operations. The data we are using for this tool is from the New York City taxi dataset, divided into month by month chunks. This is done in 2 formats, a Command Line Interface (CLI) and a Jupyter Notebook. 

## Prerequisites

To proceed to the next steps, these instructions that you have the following already installed:
GPU: NVIDIA Pascal™ or better with compute capability 6.0+
CUDA version 10.0+ (with NVIDIA drivers)
OS: Ubuntu 16.04/18.04 
Docker CE v19.03+ and nvidia-container-toolkit (if using docker container)

