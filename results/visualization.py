import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import glob
import re
from datetime import datetime
from io import StringIO

# --- Helper Functions ---

def parse_timestamp_from_filename(filename):
    """
    Extract the timestamp from the filename.
    Expected filename format: <experiment_type>_<affinity>_<dd>_<mm>_<HHMMSS>.txt
    For example: "concurrent_cpu_aff_03_04_185131.txt" where "03_04_185131" is the timestamp.
    """
    m = re.search(r'_([0-9]{2}_[0-9]{2}_[0-9]{6})\.txt$', filename)
    if m:
        ts_str = m.group(1)
        # The timestamp string contains day, month, and HHMMSS.
        dt = datetime.strptime(ts_str, "%d_%m_%H%M%S")
        # Replace the year with the current year (adjust if needed)
        dt = dt.replace(year=datetime.now().year)
        return dt
    return None

def get_latest_file(pattern):
    """
    Find files matching the glob pattern, sort them (latest first)
    using the embedded timestamp, and return the filename with the latest timestamp.
    """
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=lambda f: parse_timestamp_from_filename(f), reverse=True)
    return files[0]

def read_latest_block_from_file(filepath):
    """
    Given a text file (which may have several appended experiment runs), extract
    the CSV block corresponding to the most recent run.
    
    The file is assumed to include a header line containing "Results (Throughput MT/s):",
    then immediately a CSV header (e.g., "Threads,HashBits,Throughput") followed by the data rows.
    """
    with open(filepath, 'r') as f:
        lines = f.readlines()

    # Find all the lines that mark the beginning of an experiment block.
    block_start_indices = [i for i, line in enumerate(lines) if "Results (Throughput MT/s):" in line]

    # If none are found, assume the entire file is CSV.
    if not block_start_indices:
        return pd.read_csv(filepath)

    # Use the last block found
    latest_block_index = block_start_indices[-1]
    # The CSV header is assumed to be on the next line.
    csv_start = latest_block_index + 1

    # Determine the end of the CSV block (up to the next header or file end)
    csv_end = len(lines)
    for i in range(csv_start, len(lines)):
        if "Results (Throughput MT/s):" in lines[i]:
            csv_end = i
            break

    csv_block = "".join(lines[csv_start:csv_end])
    return pd.read_csv(StringIO(csv_block))


# --- Main Script ---

# Define the patterns for each experiment family and affinity type.
experiments = {
    "independent": {
        "no_affinity": "independent_no_affinity_*.txt",
        "cpu_affinity": "independent_cpu_aff_*.txt",
        "numa_affinity": "independent_numa_*.txt"
    },
    "concurrent": {
        "no_affinity": "concurrent_no_affinity_*.txt",
        "cpu_affinity": "concurrent_cpu_aff_*.txt",
        "numa_affinity": "concurrent_numa_*.txt"
    }
}

# We'll store the data in a nested dictionary.
data = {
    "independent": {},
    "concurrent": {}
}

# For each experiment family and affinity, pick the latest file and load its data.
for family in experiments:
    for affinity, pattern in experiments[family].items():
        file_path = get_latest_file(pattern)
        if file_path:
            df = read_latest_block_from_file(file_path)
            data[family][affinity] = df
            print(f"Loaded {family} {affinity} from file: {file_path}")
        else:
            print(f"No file found for pattern: {pattern}")

# --- Plotting ---

# Create a 2x3 subplot grid: rows represent experiment families and columns affinity types.
fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharey=True)
fig.subplots_adjust(top=0.88)

# For titling the subplots.
experiment_names = {
    "independent": "Independent",
    "concurrent": "Concurrent"
}
affinity_titles = {
    "no_affinity": "No Affinity",
    "cpu_affinity": "CPU Affinity",
    "numa_affinity": "NUMA Affinity"
}

# Choose different markers for the families:
markers = {
    "independent": "o",  # circle markers
    "concurrent": "s"    # square markers
}

# Set the plotting style.
mpl.style.use('seaborn-v0_8')

# Loop over families and affinity types.
for row_idx, family in enumerate(["independent", "concurrent"]):
    for col_idx, affinity in enumerate(["no_affinity", "cpu_affinity", "numa_affinity"]):
        ax = axes[row_idx, col_idx]
        df = data[family].get(affinity, None)
        if df is not None:
            # Determine the unique thread values for this experiment run.
            threads = sorted(df['Threads'].unique())
            # Create a color palette for the threads.
            colors = plt.cm.viridis_r(np.linspace(0, 1, len(threads)))
            
            for thread in threads:
                subset = df[df['Threads'] == thread]
                ax.plot(
                    subset['HashBits'], 
                    subset['Throughput'], 
                    marker=markers[family], 
                    color=colors[threads.index(thread)], 
                    label=f'{thread} Threads'
                )
            ax.set_xticks(range(int(df['HashBits'].min()), int(df['HashBits'].max())+1))
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.set_xlabel('Hash Bits')
            if col_idx == 0:
                ax.set_ylabel('Throughput (MT/s)')
            # Add legend for each subplot.
            ax.legend(fontsize='small')
        else:
            # If a file was not loaded, display a message.
            ax.text(0.5, 0.5, "Data not available", horizontalalignment='center', verticalalignment='center')
        
        ax.set_title(f"{experiment_names[family]} - {affinity_titles[affinity]}")

# Save and show the plot.
plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig("throughput_comparison_full.png", bbox_inches='tight')
plt.show()