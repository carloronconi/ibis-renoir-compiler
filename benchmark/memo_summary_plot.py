import argparse
import glob
import pandas as pd
import plotly.express as px

parser = argparse.ArgumentParser(description='Process some files.')
parser.add_argument('directory', type=str,
                    help='The directory to process files from')
args = parser.parse_args()

dataset_size = glob.glob(f"{args.directory}/*hyperfine_test*.json")[0].split('_')[-2]
# find the only memo_log.csv file that should be in the given directory
memo_file = glob.glob(f"{args.directory}/memo_log.csv").pop()
memo_df = pd.read_csv(memo_file)

# for each test and each backend, compute the mean values skipping the first warmup row
mean_df = (memo_df
           .groupby(['test_name', 'backend'])
           .apply(lambda x: x.iloc[1:].mean()).reset_index())

# Create a DataFrame with test names, backends, and resident_k values
df = pd.DataFrame([(row['test_name'], row['backend'], row['resident_k'] / (1024 * 1024)) for _, row in mean_df.iterrows()], columns=['Test name', 'Backend', 'Resident GB'])

# Create the scatter plot for resident_k
fig = px.scatter(df, x='Test name', y='Resident GB', color='Backend', title=f'<b>Max memory of each backend & test on dataset size {dataset_size}', labels={'Resident GB': 'Resident GB'})
fig.update_layout(legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.01), title_x=0.5, height=600, width=1000)
fig.update_yaxes(nticks=10)

# Save the plot
fig.write_image(args.directory + '/memo_summary_plot.png')

