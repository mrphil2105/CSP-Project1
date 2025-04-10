#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

def load_throughput_data(filename):
    """
    Reads a throughput file and returns a DataFrame.
    Data lines are expected in the format:
        threads,hashbits,throughput
    If a line cannot be parsed into three numeric fields (such as a repeated header),
    it is skipped.
    """
    rows = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) != 3:
                continue
            try:
                threads = int(parts[0])
                hashbits = int(parts[1])
                thrpt = float(parts[2])
            except Exception:
                continue
            rows.append({
                "Threads": threads,
                "HashBits": hashbits,
                "Throughput": thrpt
            })
    return pd.DataFrame(rows)

def main():
    results_dir = "results"
    output_dir = os.path.join("images", "throughput")
    os.makedirs(output_dir, exist_ok=True)

    # Use updated glob patterns so that files are matched correctly.
    independent_files = sorted(glob.glob(os.path.join(results_dir, "independent*results.txt")))
    concurrent_files = sorted(glob.glob(os.path.join(results_dir, "concurrent*results.txt")))
    
    print(f"Found {len(independent_files)} independent files.")
    print(f"Found {len(concurrent_files)} concurrent files.")

    # Load the data from all files so we can compute global axis limits.
    all_dfs = []
    for file in independent_files + concurrent_files:
        df = load_throughput_data(file)
        if not df.empty:
            all_dfs.append(df)
    
    if not all_dfs:
        print("No valid throughput data found in any file.")
        return

    # Combine all data to determine global limits.
    combined_df = pd.concat(all_dfs, ignore_index=True)
    global_x_min = combined_df["HashBits"].min()
    global_x_max = combined_df["HashBits"].max()
    global_y_min = combined_df["Throughput"].min()
    global_y_max = combined_df["Throughput"].max()

    # Optionally add a little margin on the axes.
    x_margin = (global_x_max - global_x_min) * 0.05 if global_x_max != global_x_min else 1
    y_margin = (global_y_max - global_y_min) * 0.05 if global_y_max != global_y_min else 1
    xlims = (global_x_min - x_margin, global_x_max + x_margin)
    ylims = (global_y_min - y_margin, global_y_max + y_margin)

    # Set up a 3x2 grid: left column for independent, right column for concurrent.
    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(14, 12))
    
    # Process files per column:
    file_groups = [(independent_files, 0), (concurrent_files, 1)]
    for file_list, col in file_groups:
        for row, filepath in enumerate(file_list):
            if row >= 3:
                continue  # Limit to 3 plots per column
            ax = axes[row][col]
            label = os.path.basename(filepath).replace("_results.txt", "")
            df = load_throughput_data(filepath)
            if df.empty:
                print(f"No valid data in {filepath}")
                continue
            for thread in sorted(df["Threads"].unique()):
                sub_df = df[df["Threads"] == thread]
                # Group by HashBits to compute average throughput.
                mean_df = sub_df.groupby("HashBits")["Throughput"].mean().reset_index()
                ax.plot(mean_df["HashBits"], mean_df["Throughput"], marker="o", label=f"Threads = {thread}")
            ax.set_title(label)
            ax.set_xlabel("HashBits")
            ax.set_ylabel("Throughput (MT/s)")
            ax.grid(True)
            ax.legend(title="Threads", fontsize="small", loc="best")
            # Set the same x and y limits for all plots.
            ax.set_xlim(xlims)
            ax.set_ylim(ylims)

    # Remove any unused axes if there are fewer than 3 plots per column.
    for row in range(3):
        for col in range(2):
            if (col == 0 and row >= len(independent_files)) or (col == 1 and row >= len(concurrent_files)):
                fig.delaxes(axes[row][col])
                
    fig.tight_layout()
    output_path = os.path.join(output_dir, "independent_vs_concurrent_throughput.svg")
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()
    print(f"Saved aggregated throughput plot to {output_path}")

if __name__ == "__main__":
    main()
