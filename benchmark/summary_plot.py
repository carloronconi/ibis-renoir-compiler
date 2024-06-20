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

# Initialize a dictionary to hold the results
results = {}

for json_file in json_files:
    backend_name = json_file.split('_')[-1].split('.json')[0]
    test_name = "_".join(json_file.split('_')[1:-1])

    with open(json_file, 'r') as f:
        data = json.load(f)

    # Add the mean time to the nested dictionary
    # There is a single result for each test
    mean_time = data["results"][0]["mean"]
    if test_name not in results:
        results[test_name] = {}
    results[test_name][backend_name] = mean_time

# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))

# Loop over the results
for i, (test_name, backend_results) in enumerate(results.items()):
    # Loop over the backends
    for backend_name, mean_time in backend_results.items():
        # Plot a dot for this backend's mean time on this test
        ax.scatter(test_name, mean_time, label=f'{backend_name} on {test_name}')

# Set the labels and title
ax.set_xlabel('Test')
ax.set_ylabel('Normalized Time')
ax.set_title('Mean Run Time of Each Backend on Each Test')

# Add a legend
ax.legend()

plt.savefig(args.directory + '/summary_plot.png')