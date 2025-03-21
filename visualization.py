import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

# Set modern style
mpl.style.use('seaborn-v0_8')

# Load CSV files
independent_df = pd.read_csv("independent_results.csv")
concurrent_df = pd.read_csv("concurrent_results.csv")

# Create subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

# Get unique thread counts from data
all_threads = sorted(pd.concat([independent_df['Threads'], 
                              concurrent_df['Threads']]).unique())
num_threads = len(all_threads)

# Custom color palette for threads
colors = plt.cm.viridis_r(np.linspace(0, 1, num_threads))

# Plot Independent results
for thread in independent_df['Threads'].unique():
    subset = independent_df[independent_df['Threads'] == thread]
    ax1.plot(subset['HashBits'], 
             subset['Throughput(MT/s)'], 
             marker='o', 
             color=colors[np.where(all_threads == thread)[0][0]],
             label=f'{thread} Threads')

ax1.set_title('(a) Independent')
ax1.set_xlabel('Hash Bits')
ax1.set_ylabel('Throughput (MT/s)')
ax1.grid(True, linestyle='--', alpha=0.7)
ax1.set_xticks(range(3, 19))

# Plot Concurrent results
for thread in concurrent_df['Threads'].unique():
    subset = concurrent_df[concurrent_df['Threads'] == thread]
    ax2.plot(subset['HashBits'], 
             subset['Throughput(MT/s)'], 
             marker='s', 
             color=colors[np.where(all_threads == thread)[0][0]],
             label=f'{thread} Threads')

ax2.set_title('(b) Concurrent')
ax2.set_xlabel('Hash Bits')
ax2.grid(True, linestyle='--', alpha=0.7)
ax2.set_xticks(range(3, 19))

# Common legend
handles, labels = ax1.get_legend_handles_labels()
fig.legend(handles, labels, loc='upper center', 
           ncol=7, bbox_to_anchor=(0.5, 1.05))

plt.tight_layout()
plt.savefig("throughput_comparison.png", bbox_inches='tight')
plt.show()