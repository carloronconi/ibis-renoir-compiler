import argparse
import glob
import json
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px

parser = argparse.ArgumentParser(description='Process some files.')
parser.add_argument('directory', type=str, help='The directory to process files from')
args = parser.parse_args()

# Find all JSON files in the specified directory that contain "hyperfine_test" in their names
# Exclude files with "COMPARISON" in their names
json_files = glob.glob(f"{args.directory}/*hyperfine_test*.json")
json_files = [file for file in json_files if "COMPARISON" not in file]

results = {}
dataset_size = json_files[0].split('_')[-2]

for json_file in json_files:
    backend_name = json_file.split('_')[-1].split('.json')[0]
    test_name = json_file.split('hyperfine_test_')[-1].rsplit('_', 2)[0]

    with open(json_file, 'r') as f:
        data = json.load(f)

    # Add the mean time to the nested dictionary
    # There is a single result for each test
    mean_time = data["results"][0]["mean"]
    if test_name not in results:
        results[test_name] = {}
    results[test_name][backend_name] = mean_time

# Define a color map for the backends
cmap = plt.get_cmap('viridis')
backend_names = results[results.keys().__iter__().__next__()].keys()
color_map = {b: cmap(i / len(backend_names)) for i, b in enumerate(backend_names)}

fig, ax = plt.subplots(figsize=(10, 6))

# Sort the results dictionary by test_name
results = dict(sorted(results.items()))

for i, (test_name, backend_results) in enumerate(results.items()):
    for backend_name, mean_time in backend_results.items():
        # Use the color map to set the color of the dot
        ax.scatter(test_name, mean_time, color=color_map[backend_name], label=f'{backend_name} on {test_name}')

# Create a custom legend
handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) for backend, color in color_map.items()]
ax.legend(handles, color_map.keys(), loc='upper left', bbox_to_anchor=(1, 1))

ax.set_xlabel('Test name')
ax.set_ylabel('Time [s]')
ax.set_title(f'Mean run time of each backend & test on dataset size {dataset_size}')

# Rotate x-axis labels
plt.xticks(rotation=45, ha='right')
# Adjust the bottom margin to make more space for the x-axis labels
plt.subplots_adjust(bottom=0.5, right=0.8)

plt.savefig(args.directory + '/plt_std_summary_plot.png')

# Normalize the results
normalized_results = {test_name: {backend_name: mean_time / max(backend_results.values()) for backend_name, mean_time in backend_results.items()} for test_name, backend_results in results.items()}

# Create a new figure for the normalized plot
fig_normalized, ax_normalized = plt.subplots(figsize=(10, 6))

# Plot the normalized results
for i, (test_name, backend_results) in enumerate(normalized_results.items()):
    for backend_name, mean_time in backend_results.items():
        # Use the color map to set the color of the dot
        ax_normalized.scatter(test_name, mean_time, color=color_map[backend_name], label=f'{backend_name} on {test_name}')

# Create a custom legend
handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) for backend, color in color_map.items()]
ax_normalized.legend(handles, color_map.keys(), loc='upper left', bbox_to_anchor=(1, 1))

ax_normalized.set_xlabel('Test name')
ax_normalized.set_ylabel('Normalized Time')
ax_normalized.set_title(f'Normalized mean run time of each backend & test on dataset size {dataset_size}')

# Rotate x-axis labels
plt.xticks(rotation=45, ha='right')
# Adjust the bottom margin to make more space for the x-axis labels
plt.subplots_adjust(bottom=0.5, right=0.8)

plt.savefig(args.directory + '/plt_norm_summary_plot.png')

# Convert results to a DataFrame
df = pd.DataFrame([(test_name, backend_name, mean_time) for test_name, backend_results in results.items() for backend_name, mean_time in backend_results.items()], columns=['Test name', 'Backend', 'Time'])
df_normalized = pd.DataFrame([(test_name, backend_name, mean_time) for test_name, backend_results in normalized_results.items() for backend_name, mean_time in backend_results.items()], columns=['Test name', 'Backend', 'Normalized Time'])

# Create the scatter plot
fig = px.scatter(df, x='Test name', y='Time', color='Backend', title=f'<b>Mean run time of each backend & test on dataset size {dataset_size}', labels={'Time': 'Time [s]'})
fig.update_layout(legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.01), title_x=0.5, height=600, width=1000)
fig.update_yaxes(nticks=10)

# Save the plot
fig.write_image(args.directory + '/px_std_summary_plot.png')

# Create the normalized scatter plot
fig_normalized = px.scatter(df_normalized, x='Test name', y='Normalized Time', color='Backend', title=f'<b>Normalized mean run time of each backend & test on dataset size {dataset_size}', labels={'Normalized Time': 'Normalized Time'})
fig_normalized.update_layout(legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.01), title_x=0.5, height=600, width=1000)

# Save the normalized plot
fig_normalized.write_image(args.directory + '/px_norm_summary_plot.png')