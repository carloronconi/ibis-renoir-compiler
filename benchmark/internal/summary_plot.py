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
    df = pd.read_csv(file, dtype={'exception': 'str'}, na_values=['None'])


    # remove warmup runs, but keep those that failed
    agg = df[(df['run_count'] != -1) | df['exception'].notna()].groupby(['test_name', 'backend_name', 'table_origin']).agg({
        'total_time_s': ['mean', 'std'],
        'max_memory_MiB': ['mean', 'std'],
        'run_count': 'size',
        'exception': 'first'
    })

    # find the most common number of runs - a few tests could have failed and have fewer runs
    test_runs = mode(agg['run_count']['size'].tolist())

    agg_reset = agg.reset_index()
    agg_reset['backend_table_comb'] = agg_reset['backend_name'] + ' + ' + agg_reset['table_origin']
    agg_reset.columns = [('_'.join(col).strip() if col[0] in ['total_time_s', 'max_memory_MiB', 'exception'] else col[0]) for col in agg_reset.columns.values]

    agg_reset.loc[agg_reset['exception_first'].str.startswith('Traceback', na=False), 'exception_first'] = 'raise'

    # Remove invalid times so that they're not shown in the plot
    agg_reset.loc[(agg_reset['exception_first'] == 'timeout'), 'total_time_s_mean'] = -20
    agg_reset.loc[(agg_reset['exception_first'] == 'raise'), 'total_time_s_mean'] = -10
    agg_reset.loc[(agg_reset['max_memory_MiB_mean'] < 0), 'max_memory_MiB_mean'] = None

    fig = make_subplots(rows=2, cols=2, subplot_titles=('Cached', 'CSV'),
                        vertical_spacing=0.01, horizontal_spacing=0.01,
                        shared_xaxes='all', shared_yaxes='rows')

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
    
    for trace in time.data:
        col = 1 if trace.yaxis == "y" else 2
        fig.add_trace(trace, row=1, col=col)
    for trace in memo.data:
        col = 1 if trace.yaxis == "y" else 2
        fig.add_trace(trace, row=2, col=col)

    # fig.update_layout(height=1000)
    # fig.update_xaxes(title_text="Test Name", row=2, col=1)
    # fig.update_xaxes(title_text="Test Name", row=2, col=2)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=2)
    fig.update_yaxes(title_text="Total Time (s)", row=1, col=1)
    fig.update_yaxes(title_text="Max Memory (MiB)", row=2, col=1)
    fig.update_layout(margin=dict(l = 20, r = 20, t = 100, b = 10), title_text=f"<b>Backends comparison over tests: total time and max memory over {dataset_size} dataset and {test_runs} runs<b>")

    fig.show()

if __name__ == "__main__":
    main()