import argparse
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
from statistics import mode

def main():
    parser = argparse.ArgumentParser(description='Plot summary of internal benchmark run.')
    parser.add_argument('dir', type=str, help='The directory containing the internal benchmark results')
    args = parser.parse_args()

    dataset_size = args.dir.split('/')[-1].split('_')[0]
    file = args.dir + "/codegen_log.csv"
    df = pd.read_csv(file)
    col_groups = ['test_name', 'backend_name', 'table_origin']
    # find the most common number of runs - a few tests could have failed and have fewer runs
    test_runs = mode(df.groupby(col_groups).size().reset_index(name='run_count')['run_count'].tolist())
    agg = df[df['run_count'] != -1].groupby(col_groups).agg({
        'total_time_s': ['mean', 'std'],
        'max_memory_MiB': ['mean', 'std']
    })

    agg_reset = agg.reset_index()
    agg_reset['backend_table_comb'] = agg_reset['backend_name'] + ' + ' + agg_reset['table_origin']
    agg_reset.columns = [('_'.join(col).strip() if col[0] in ['total_time_s', 'max_memory_MiB'] else col[0]) for col in agg_reset.columns.values]

    fig = make_subplots(rows=2, cols=2, subplot_titles=('Cached', 'CSV'),
                        vertical_spacing=0.01, horizontal_spacing=0.02,
                        shared_xaxes=True, shared_yaxes=True)

    time = px.scatter(agg_reset, x='test_name', y='total_time_s_mean', color='backend_name',
                     facet_col='table_origin',
                     labels={'test_name': 'Test Name', 'total_time_s_mean': 'Mean Total Time (s)', 'backend_name': 'Backend'},
                     title='Mean Total Time per Test by Table Origin and Backend',
                     error_y='total_time_s_std')
    
    memo = px.scatter(agg_reset, x='test_name', y='max_memory_MiB_mean', color='backend_name',
                     facet_col='table_origin',
                     labels={'test_name': 'Test Name', 'max_memory_MiB_mean': 'Mean Max Memory (MiB)', 'backend_name': 'Backend'},
                     title='Mean Max Memory per Test by Table Origin and Backend',
                     error_y='max_memory_MiB_std')
    
    for i, trace in enumerate(time.data):
        fig.add_trace(trace, row=1, col=(i % 2) + 1)
    for i, trace in enumerate(memo.data):
        fig.add_trace(trace, row=2, col=(i % 2) + 1)

    # fig.update_layout(height=1000)
    # fig.update_xaxes(title_text="Test Name", row=2, col=1)
    # fig.update_xaxes(title_text="Test Name", row=2, col=2)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=2)
    fig.update_yaxes(title_text="Total Time (s)", row=1, col=1)
    fig.update_yaxes(title_text="Max Memory (MiB)", row=2, col=1)
    fig.update_layout(margin=dict(l = 20, r = 20, t = 100, b = 10), title_text=f"<b>Benchmark Results: Total Time and Max Memory over {dataset_size} dataset and {test_runs} runs<b>")

    fig.show()

if __name__ == "__main__":
    main()