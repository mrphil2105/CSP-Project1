#!/usr/bin/env python3
import re
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import math

def parse_perf_file(filename):
    """
    Parses an aggregated perf file.
    Searches for header lines containing the threads and hashbits (e.g.:
      >>> Running ... with 1 threads and 2 hashbits)
    and then parses the subsequent block of performance counter stats.
    Returns a DataFrame with columns: Threads, HashBits, Metric, Value.
    """
    records = []
    current_threads = None
    current_hashbits = None

    with open(filename) as f:
        lines = f.readlines()

    # Pattern to match header lines that specify threads and hashbits.
    header_re = re.compile(r">>> Running .* with (\d+)\s+threads\s+and\s+(\d+)\s+hashbits")
    # Pattern to match lines with a performance counter value, e.g.:
    #     5,305,595,199      cpu-cycles  ( +-  0.06% )
    metric_re = re.compile(r"([\d,]+)\s+(\S+)")
    
    in_perf_block = False

    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        header_match = header_re.search(line)
        if header_match:
            current_threads = int(header_match.group(1))
            current_hashbits = int(header_match.group(2))
            in_perf_block = False
            continue

        # Start of a performance counter block.
        if line.startswith("Performance counter stats"):
            in_perf_block = True
            continue
        
        if in_perf_block:
            if "seconds time elapsed" in line:
                in_perf_block = False
                continue
            
            metric_match = metric_re.search(line)
            if metric_match and current_threads is not None and current_hashbits is not None:
                raw_value = metric_match.group(1).replace(",", "")
                try:
                    value = float(raw_value)
                except Exception:
                    continue
                metric_name = metric_match.group(2)
                records.append({
                    "Threads": current_threads,
                    "HashBits": current_hashbits,
                    "Metric": metric_name,
                    "Value": value
                })
    return pd.DataFrame(records)

def plot_perf_grid(df, experiment_label, output_file):
    """
    Plots all performance metrics for an experiment in a grid of subplots.
    For each metric, a line plot of Value vs HashBits is drawn,
    with a separate line for each Threads value.
    """
    metrics = sorted(df["Metric"].unique())
    num_metrics = len(metrics)
    # Decide on grid dimensions (two columns; rows as needed)
    ncols = 2
    nrows = math.ceil(num_metrics / ncols)

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(ncols*6, nrows*4))
    # Flatten axes for easier iteration.
    if nrows * ncols == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        sub_df = df[df["Metric"] == metric]
        for thread in sorted(sub_df["Threads"].unique()):
            thread_df = sub_df[sub_df["Threads"] == thread]
            mean_df = thread_df.groupby("HashBits")["Value"].mean().reset_index()
            ax.plot(mean_df["HashBits"], mean_df["Value"], marker="o", label=f"Threads = {thread}")
        ax.set_title(metric)
        ax.set_xlabel("HashBits")
        ax.set_ylabel("Value")
        ax.grid(True)
        ax.legend(title="Threads", fontsize='small', loc='best')
    
    # Remove any unused subplots.
    for j in range(idx + 1, nrows * ncols):
        fig.delaxes(axes[j])
        
    fig.suptitle(f"Performance Metrics for {experiment_label}", fontsize=16)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(output_file, bbox_inches="tight")
    plt.close()

def main():
    perf_dir = "perf"
    output_dir = os.path.join("images", "perf")
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each perf file in the perf folder.
    for filepath in glob.glob(os.path.join(perf_dir, "*_perf.txt")):
        experiment_label = os.path.basename(filepath).replace("_perf.txt", "")
        df = parse_perf_file(filepath)
        if df.empty:
            print(f"No valid perf data found in {filepath}")
            continue
        output_file = os.path.join(output_dir, f"{experiment_label}_perf.svg")
        plot_perf_grid(df, experiment_label, output_file)
        print(f"Saved perf grid plot for {experiment_label} to {output_file}")

if __name__ == "__main__":
    main()
