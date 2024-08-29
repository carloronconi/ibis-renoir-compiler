import pandas as pd
import sys
import plotly.express as px
import plotly.graph_objects as go
import os

if len(sys.argv) < 2:
    print("Please provide the path to the CSV file as an argument.")
    sys.exit(1)
csv_path = sys.argv[1]

df = pd.read_csv(csv_path)
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M%S')
df['memory_GB'] = df['memory_KB'] / 1_000_000

# Calculate elapsed time from the beginning
start_time = df['timestamp'].iloc[0]
df['elapsed_time_min'] = (df['timestamp'] - start_time).dt.total_seconds() / 60  # Convert to minutes

csv_dir = os.path.dirname(csv_path)
codegen_log_path = os.path.join(csv_dir, 'codegen_log.csv')
codegen_df = pd.read_csv(codegen_log_path)
codegen_df['timestamp'] = pd.to_datetime(codegen_df['timestamp'], format='%Y-%m-%d %H:%M:%S.%f')

# Calculate adjusted timestamps for codegen_log
codegen_df['adjusted_timestamp'] = codegen_df['timestamp'] - pd.to_timedelta(codegen_df['total_time_s'], unit='s')
codegen_df['elapsed_time_min'] = (codegen_df['adjusted_timestamp'] - start_time).dt.total_seconds() / 60  # Convert to minutes

change_points = codegen_df[codegen_df['test_name'].shift() != codegen_df['test_name']]

fig = px.line(df, x='elapsed_time_min', y='memory_GB', title='Memory Consumption Over Time (GB)')

# Add the first test name at the beginning
first_test_name = codegen_df.iloc[0]['test_name']
first_test_time = codegen_df.iloc[0]['adjusted_timestamp']
first_test_elapsed_time = (first_test_time - start_time).total_seconds() / 60  # Convert to minutes
label_y = df['memory_GB'].max() + 1
fig.add_annotation(
    x=first_test_elapsed_time,
    y=label_y,
    text=first_test_name,
    showarrow=False,
    arrowhead=0,
    ax=0,
    ay=-40,
    bgcolor="white",
    textangle=270
)

# Add vertical lines and annotations for each change point
for idx, row in change_points.iterrows():
    elapsed_time_min = row['elapsed_time_min']
    test_name = row['test_name']
    fig.add_shape(
        type="line",
        x0=elapsed_time_min,
        y0=0,
        x1=elapsed_time_min,
        y1=1,
        xref='x',
        yref='paper',
        line=dict(color="red", width=2)
    )
    fig.add_annotation(
        x=elapsed_time_min,
        y=label_y,
        text=test_name,
        showarrow=False,
        arrowhead=0,
        ax=0,
        ay=-40,
        bgcolor="white",
        textangle=270
    )

# Update layout to increase font size for axis labels and title
fig.update_layout(
    title_font=dict(size=26),
    xaxis_title_font=dict(size=20),
    yaxis_title_font=dict(size=20),
    font=dict(size=18)  # Increase font size for tick labels
)

fig.show()