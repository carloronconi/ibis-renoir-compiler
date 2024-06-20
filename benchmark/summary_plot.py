import argparse
import glob
import json
import matplotlib.pyplot as plt

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
    test_name = json_file.split('hyperfine_')[-1].rsplit('_', 2)[0]

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

for i, (test_name, backend_results) in enumerate(results.items()):
    for backend_name, mean_time in backend_results.items():
        # Use the color map to set the color of the dot
        ax.scatter(test_name, mean_time, color=color_map[backend_name], label=f'{backend_name} on {test_name}')

# Create a custom legend
handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10) for backend, color in color_map.items()]
ax.legend(handles, color_map.keys())

ax.set_xlabel('Test')
ax.set_ylabel('Time')
ax.set_title(f'Mean Run Time of Each Backend & Test on Dataset Size {dataset_size}')

# Rotate x-axis labels
plt.xticks(rotation=45, ha='right')
# Adjust the bottom margin to make more space for the x-axis labels
plt.subplots_adjust(bottom=0.5)

plt.savefig(args.directory + '/summary_plot.png')