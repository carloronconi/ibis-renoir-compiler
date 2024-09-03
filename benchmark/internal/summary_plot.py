import argparse
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots

def main():
    parser = argparse.ArgumentParser(description='Plot summary of internal benchmark run.')
    parser.add_argument('dir', type=str, help='The directory containing the internal benchmark results')
    parser.add_argument('--time-only', action='store_true', help='Only draw the time part of the plot')
    parser.add_argument('--sum-pre', action='store_true', help='Add pre-query time to the post-query time')
    parser.add_argument('--backends', type=str, help='Comma-separated list of backend names to include in the plot')
    parser.add_argument('--test-patterns', type=str, help='Comma-separated list of test pattern strings to filter test names')
    args = parser.parse_args()

    dataset_size = args.dir.split('/')[-1].split('_')[0]
    file = args.dir + "/codegen_log.csv"
    df = pd.read_csv(file, dtype={'exception': 'str'}, na_values=['None'])

    # if sum-pre is selected, add pre_query_time_s to total_time_s for tests that have a positive pre_query_time_s
    if args.sum_pre:
        df.loc[df['pre_query_time_s'] > 0, 'total_time_s'] += df['pre_query_time_s']
        df.loc[df['pre_query_time_s'] > 0, 'max_memory_MiB'] += df['pre_query_memo_MiB']

    # remove warmup runs, but keep those that failed
    agg = df[(df['run_count'] != -1) | df['exception'].notna()].groupby(['test_name', 'backend_name', 'scenario']).agg({
        'total_time_s': ['mean', 'std'],
        'max_memory_MiB': ['mean', 'std'],
        'run_count': 'size',
        'exception': 'first'
    })

    # find the max number of runs - a few tests could have failed and have fewer runs
    test_runs = max(agg['run_count']['size'].tolist())

    agg_reset = agg.reset_index()
    agg_reset['backend_table_comb'] = agg_reset['backend_name'] + ' + ' + agg_reset['scenario']
    agg_reset.columns = [('_'.join(col).strip() if col[0] in ['total_time_s', 'max_memory_MiB', 'exception'] else col[0]) for col in agg_reset.columns.values]

    agg_reset.loc[agg_reset['exception_first'].str.startswith('Traceback', na=False), 'exception_first'] = 'raise'

    # rename test names adding the scenario as prefix so two scenarios running the same test result in different items plotted
    for idx, row in agg_reset.iterrows():
        if row['scenario'] == 'Scenario3baseline':
            agg_reset.at[idx, 'test_name'] = 'Scenario3: ' + row['test_name']
            agg_reset.at[idx, 'backend_name'] = row['backend_name'] + '-os'
        else:
            agg_reset.at[idx, 'test_name'] = row['scenario'] + ': ' + row['test_name']

    # Remove invalid times so that they're not shown in the plot
    agg_reset.loc[(agg_reset['exception_first'] == 'timeout'), 'total_time_s_mean'] = -20
    agg_reset.loc[(agg_reset['exception_first'] == 'raise'), 'total_time_s_mean'] = -10
    agg_reset.loc[(agg_reset['max_memory_MiB_mean'] < 0), 'max_memory_MiB_mean'] = None

    # Define a color mapping for backends
    backend_colors = {
        'renoir': 'rgb(172, 128, 160)',
        'renoir-os': 'rgba(172, 128, 160, 0.6)',
        'duckdb': 'rgb(255, 209, 102)',
        'duckdb-os': 'rgba(255, 209, 102, 0.6)',
        'polars': 'rgb(17, 138, 178)',
        'polars-os': 'rgba(17, 138, 178, 0.6)',
        'flink': 'rgb(239, 71, 111)',
        'flink-os': 'rgba(239, 71, 111, 0.6)',
        'spark': 'rgb(247, 140, 107)',
        'spark-os': 'rgba(247, 140, 107, 0.6)',
        'risingwave': 'rgb(7, 59, 76)',
        'risingwave-os': 'rgba(7, 59, 76, 0.6)',
    }

    # Filter backends if the --backends argument is provided
    if args.backends:
        selected_backends = args.backends.split(',')
        agg_reset = agg_reset[agg_reset['backend_name'].apply(lambda x: any(bac in x for bac in selected_backends))]

    # Filter test names if the --test-patterns argument is provided
    if args.test_patterns:
        patterns = args.test_patterns.split(',')
        agg_reset = agg_reset[agg_reset['test_name'].apply(lambda x: any(pat in x for pat in patterns))]

    # Sort by test_name alphabetically
    agg_reset = agg_reset.sort_values(by='test_name')

    if args.time_only:
        fig = make_subplots(rows=1, cols=1, vertical_spacing=0.01, horizontal_spacing=0.01)
        opt_title = ""
    else:
        fig = make_subplots(rows=2, cols=1, vertical_spacing=0.01, horizontal_spacing=0.01, shared_xaxes='all', shared_yaxes='rows')
        opt_title = " and memory usage"

    time = px.bar(agg_reset, x='test_name', y='total_time_s_mean', color='backend_name', barmode='group',
                  labels={'test_name': 'Test Name', 'total_time_s_mean': 'Mean Total Time (s)', 'backend_name': 'Backend'},
                  title='Mean Total Time per Test by Table Origin and Backend',
                  error_y='total_time_s_std',
                  color_discrete_map=backend_colors)

    if not args.time_only:
        memo = px.bar(agg_reset, x='test_name', y='max_memory_MiB_mean', color='backend_name', barmode='group',
                      labels={'test_name': 'Test Name', 'max_memory_MiB_mean': 'Mean Max Memory (MiB)', 'backend_name': 'Backend'},
                      title='Mean Max Memory per Test by Table Origin and Backend',
                      error_y='max_memory_MiB_std',
                      color_discrete_map=backend_colors)

    for trace in time.data:
        fig.add_trace(trace, row=1, col=1)
    if not args.time_only:
        for trace in memo.data:
            trace.showlegend = False
            fig.add_trace(trace, row=2, col=1)

    fig.update_xaxes(showticklabels=True if args.time_only else False, row=1, col=1, showgrid=True)
    if not args.time_only:
        fig.update_xaxes(showticklabels=True, row=2, col=1)
        fig.update_yaxes(title_text="Max Memory (MiB)", row=2, col=1)
    fig.update_yaxes(title_text="Total Time (s)", row=1, col=1)
    fig.update_layout(
        margin=dict(l=20, r=20, t=100, b=10), 
        title_text=f"<b>Total time{opt_title}<br>{dataset_size} dataset over {test_runs} runs<b>",
        title_font=dict(size=26),
        xaxis_title_font=dict(size=20),
        yaxis_title_font=dict(size=20),
        font=dict(size=18),
        )

    fig.show()

if __name__ == "__main__":
    main()