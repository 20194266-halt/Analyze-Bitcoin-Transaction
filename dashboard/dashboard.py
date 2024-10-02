import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import pyarrow.parquet as pq
import os
import plotly.graph_objects as go
output_delta_table_path = "/tmp/delta/btc_transactions_output" 

def read_delta_table(delta_table_path):
    parquet_files = [
        f for f in os.listdir(delta_table_path) if f.endswith(".parquet")
    ]
    dfs = [pq.read_table(os.path.join(delta_table_path, file)).to_pandas() for file in parquet_files]
    return pd.concat(dfs, ignore_index=True)

df_pandas = read_delta_table(output_delta_table_path)

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Bitcoin Transactions Dashboard", style={'text-align': 'center'}),

    html.Div(style={'display': 'flex', 'flexWrap': 'wrap', 'width': '100%', 'height': '100vh'}, children=[
        html.Div([
            dcc.Graph(id='histogram-graph', style={'height': '100%', 'width': '100%'}),
        ], style={'width': '50%', 'height': '50%'}),  # Histogram

        html.Div([
            dcc.Graph(id='line-graph', style={'height': '100%', 'width': '100%'}),
        ], style={'width': '50%', 'height': '50%'}),  # Line graph

        html.Div([
            dcc.Graph(id='pie-chart', style={'height': '100%', 'width': '100%'}),
        ], style={'width': '50%', 'height': '50%'}),  # Pie chart

        html.Div([
            html.H2("Transaction Data Table"),
            dash.dash_table.DataTable(
                id='data-table',
                columns=[{"name": i, "id": i} for i in df_pandas.columns],
                data=df_pandas.to_dict('records'),
                page_size=10,
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left'},
                style_header={'fontWeight': 'bold'}
            )
        ], style={'width': '50%', 'height': '50%'})  # Table
    ])
])

@app.callback(
    Output('histogram-graph', 'figure'),
    Input('histogram-graph', 'id')
)
def update_histogram(_):
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=df_pandas['transaction_size'], name='Transaction Size', marker_color='blue'))
    fig.add_trace(go.Histogram(x=df_pandas['prev_output_value'], name='Prev Output Value', marker_color='green'))
    fig.add_trace(go.Histogram(x=df_pandas['output_value'], name='Output Value', marker_color='orange'))

    fig.update_layout(
        barmode='overlay',
        title="Distribution of Transaction Size, Prev Output Value, and Output Value",
        xaxis_title="Value",
        yaxis_title="Count"
    )
    fig.update_traces(opacity=0.75)
    return fig

@app.callback(
    Output('line-graph', 'figure'),
    Input('line-graph', 'id')
)
def update_line_graph(_):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_pandas['transaction_time'], y=df_pandas['vin_sz'], mode='lines', name='VIN Size', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=df_pandas['transaction_time'], y=df_pandas['vout_sz'], mode='lines', name='VOUT Size', line=dict(color='green')))

    fig.update_layout(
        title="VIN Size and VOUT Size Change Over Time",
        xaxis_title="Time",
        yaxis_title="Size",
        showlegend=True
    )
    return fig

@app.callback(
    Output('pie-chart', 'figure'),
    Input('pie-chart', 'id')
)
def update_pie_chart(_):
    double_spending_counts = df_pandas['double_spend'].value_counts()
    fig = px.pie(
        values=double_spending_counts.values,
        names=double_spending_counts.index,
        title="Double Spending: True vs False",
        color_discrete_sequence=['green', 'red']
    )
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
