import argparse
import pandas as pd
import plotly.express as px

def main():
    parser = argparse.ArgumentParser(description='Plot summary of internal benchmark run.')
    parser.add_argument('dir', type=str, help='The directory containing the internal benchmark results')
    parser.add_argument('--plot-type', choices=['backend', 'origin'], default='origin', help='Choose the plot type: "backend" or "origin"')
    args = parser.parse_args()

    file = args.dir + "/codegen_log.csv"
    df = pd.read_csv(file)
    agg = df[df['run_count'] != -1].groupby(['test_name', 'backend_name', 'table_origin']).agg({
        'total_time_s': ['mean', 'std'],
        'max_memory_B': ['mean', 'std']
    })

    agg_reset = agg.reset_index()
    agg_reset['backend_table_comb'] = agg_reset['backend_name'] + ' + ' + agg_reset['table_origin']
    agg_reset.columns = [('_'.join(col).strip() if col[0] in ['total_time_s', 'max_memory_B'] else col[0]) for col in agg_reset.columns.values]

    if args.plot_type == 'backend':
        fig = px.scatter(agg_reset, x='test_name', y='total_time_s_mean', color='table_origin',
                         facet_col='backend_name',
                         labels={'test_name': 'Test Name', 'total_time_s_mean': 'Mean Total Time (s)', 'table_origin': 'Table Origin'},
                         title='Mean Total Time per Test by Backend and Table Origin',
                         error_y='total_time_s_std')
    elif args.plot_type == 'origin':
        fig = px.scatter(agg_reset, x='test_name', y='total_time_s_mean', color='backend_name',
                         facet_col='table_origin',
                         labels={'test_name': 'Test Name', 'total_time_s_mean': 'Mean Total Time (s)', 'backend_name': 'Backend'},
                         title='Mean Total Time per Test by Table Origin and Backend',
                         error_y='total_time_s_std')
        
    fig.update_layout(xaxis_title='Test Name',
                      yaxis_title='Mean Total Time (s)',
                      legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5))
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.show()

if __name__ == "__main__":
    main()