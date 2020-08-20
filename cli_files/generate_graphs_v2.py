import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt 
import numpy as np

def generate_cudf_vs_pandas_p1(perf_data):
    copy_perf_data_p1 = perf_data 
    perf_data_p1 = copy_perf_data_p1[copy_perf_data_p1['operation'].isin(["af", "aggfunc", "gb", "dc", "fc", "fnv", "merge"])]
    library = perf_data_p1['library']
    colors = ['green' if l == 'cuDF' else 'grey' for l in library]
    sns_plot_p1 = sns.barplot(x='operation', y='time (in s)', hue='library', data=perf_data_p1, palette=colors, ci=None)
    plt.title("Pandas/cuDF operation vs. Time - Part I ")
    sns_plot_p1.figure.savefig("cudf_vs_pandas_p1.png")
    plt.figure()
    
def generate_cudf_vs_pandas_p2(perf_data):
    copy_perf_data_p2 = perf_data 
    perf_data_p2 = copy_perf_data_p2[copy_perf_data_p2['operation'].isin(["ld", "rtc", "dd", "gt5"])]
    library = perf_data_p2['library']
    colors = ['green' if l == 'cuDF' else 'grey' for l in library]
    sns_plot_p2 = sns.barplot(x='operation', y='time (in s)', hue='library', data=perf_data_p2, palette=colors, ci=None)
    plt.title("Pandas/cuDF operation vs. Time - Part II ")
    sns_plot_p2.figure.savefig("cudf_vs_pandas_p2.png")
    plt.figure()
    
def generate_speedups(cudf_barplot_data):
    speedups_df = pd.DataFrame(np.array(cudf_barplot_data), columns=['operation', 'speedups (X times faster than pandas)'])
    speedups_df['speedups (X times faster than pandas)'] = speedups_df['speedups (X times faster than pandas)'].astype(float)
    colors = ['green' for i in range(len(speedups_df))]
    sns_plot_speedups = sns.barplot(x='operation', y='speedups (X times faster than pandas)', data=speedups_df, palette=colors, ci=None)
    plt.title("cuDF Speedup vs. Pandas")
    sns_plot_speedups.figure.savefig("speedups.png")
    plt.figure()
    