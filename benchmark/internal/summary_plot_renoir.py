import argparse
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots

def main():
    parser = argparse.ArgumentParser(description='Plot summary of internal benchmark run - renoir compile + execute breakdown.')
    parser.add_argument('dir', type=str, help='The directory containing the internal benchmark results')
    args = parser.parse_args()

    file = args.dir + "/codegen_log.csv"
    df = pd.read_csv(file, dtype={'exception': 'str'}, na_values=['None'])

    # remove warmup runs, but keep those that failed
    agg = df[(df['run_count'] != -1) | df['exception'].notna()].groupby(['test_name', 'backend_name', 'scenario']).agg({
        'renoir_compile_time_s': ['mean', 'std'],
        'renoir_execute_time_s': ['mean', 'std'],
        'total_time_s': ['mean', 'std'],
        'max_memory_MiB': ['mean', 'std'],
        'run_count': 'size',
        'exception': 'first'
    })

    agg_reset = agg.reset_index()
    agg_reset['backend_table_comb'] = agg_reset['backend_name'] + ' + ' + agg_reset['scenario']
    agg_reset.columns = [('_'.join(col).strip() if col[0] in ['renoir_compile_time_s', 'renoir_execute_time_s', 'total_time_s', 'max_memory_MiB', 'exception'] 
                          else col[0]) for col in agg_reset.columns.values]

    agg_reset.loc[agg_reset['exception_first'].str.startswith('Traceback', na=False), 'exception_first'] = 'raise'

    # rename test names adding the scenario as prefix so two scenarios running the same test result in different items plotted
    for idx, row in agg_reset.iterrows():
        agg_reset.at[idx, 'test_name'] = row['scenario'] + ': ' + row['test_name']

    # Remove invalid times so that they're not shown in the plot
    agg_reset.loc[(agg_reset['exception_first'] == 'timeout'), 'total_time_s_mean'] = -20
    agg_reset.loc[(agg_reset['exception_first'] == 'raise'), 'total_time_s_mean'] = -10
    agg_reset.loc[(agg_reset['max_memory_MiB_mean'] < 0), 'max_memory_MiB_mean'] = None

    # Define a color mapping for backends
    backend_colors = {
        'renoir_compile_time_s_mean': 'rgb(172, 128, 160)',
        'renoir_execute_time_s_mean': 'rgb(172, 128, 160)'
    }
    backend_styles = {
        'renoir_compile_time_s_mean': '',
        'renoir_execute_time_s_mean': '/'
    }

    agg_reset = agg_reset[agg_reset['backend_name'].apply(lambda x: "renoir" in x)]

    # Sort by test_name alphabetically
    agg_reset = agg_reset.sort_values(by='test_name')

    # Create the plots
    fig = make_subplots(rows=1, cols=1, vertical_spacing=0.01, horizontal_spacing=0.01)

    time = px.bar(
        agg_reset.melt(id_vars=['test_name'], value_vars=['renoir_compile_time_s_mean', 'renoir_execute_time_s_mean'], 
                       var_name='time_type', value_name='time'),
        x='test_name', 
        y='time', 
        color='time_type', 
        barmode='stack',
        color_discrete_map=backend_colors,
        pattern_shape='time_type',
        pattern_shape_map=backend_styles,
        labels={'test_name': 'Test Name', 'time': 'Time (s)', 'time_type': 'Time Type'},
        title='Compile and Execute Time Breakdown per Test',
        text='time'
    )
    time.update_traces(texttemplate='%{text:.3f}', textposition='outside', textangle=0, textfont=dict(size=16))

    for trace in time.data:
        fig.add_trace(trace, row=1, col=1)

    fig.update_xaxes(showticklabels=True, row=1, col=1, showgrid=True)
    fig.update_yaxes(title_text='Total Time (s)', row=1, col=1)
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=10), 
        font=dict(size=18),
        barmode='stack'
        )

    fig.show()

if __name__ == "__main__":
    main()
