import json
import streamlit as st
import plotly.graph_objects as go

# Load lineage data from JSON file
def load_lineage_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

# Recursive function to build lineage nodes and edges
def build_lineage_graph(data, selected_table, selected_column):
    nodes = []
    edges = []
    visited = set()
    node_info = {}  # Dictionary to store tooltips for each node

    def add_node(table, column, description, transformation):
        node_id = f"{table}.{column}"
        if node_id not in visited:
            hover_text = f"Description: {description}<br>Transformation: {transformation}"
            nodes.append({'id': node_id, 'label': f"{table}.{column}", 'hover_text': hover_text})
            visited.add(node_id)

    def add_edge(source_table, source_column, target_table, target_column):
        source_id = f"{source_table}.{source_column}"
        target_id = f"{target_table}.{target_column}"
        edges.append({'source': source_id, 'target': target_id})

    def build_graph(table, column):
        # Find the current node in the data
        current_node = next((item for item in data if item['model'] == table and item['column'] == column), None)
        if current_node:
            description = current_node.get('column Description', 'Description not available')
            transformation = current_node.get('reasoning', 'Transformation not available')
            add_node(table, column, description, transformation)
            
            # Recursively add upstream models
            for upstream_model in current_node['upstream_models']:
                upstream_table = upstream_model['model']
                upstream_column = upstream_model['column']
                upstream_description = upstream_model.get('column Description', 'Description not available')
                upstream_transformation = upstream_model.get('reasoning', 'Transformation not available')
                add_node(upstream_table, upstream_column, upstream_description, upstream_transformation)
                add_edge(upstream_table, upstream_column, table, column)
                build_graph(upstream_table, upstream_column)

    build_graph(selected_table, selected_column)
    return nodes, edges

# Load data from JSON file
file_path = 'lineage.json'  # Ensure the file is in the same directory
data = load_lineage_data(file_path)

# Extract unique table and column names from data
tables = list(set(item['model'] for item in data))
columns_by_table = {table: list(set(item['column'] for item in data if item['model'] == table)) for table in tables}

# Streamlit app layout
st.title('Data Lineage Graph')

# Dropdown for table selection
selected_table = st.selectbox('Select a Table', options=tables)

# Dropdown for column selection based on selected table
selected_column = st.selectbox('Select a Column', options=columns_by_table[selected_table])

# Build lineage graph
nodes, edges = build_lineage_graph(data, selected_table, selected_column)

# Create node trace with tooltips
node_trace = go.Scatter(
    x=[i for i in range(len(nodes))], y=[i for i in range(len(nodes))],
    mode='markers+text',
    text=[node['label'] for node in nodes],
    textposition="top center",
    hoverinfo='text',
    hovertext=[node['hover_text'] for node in nodes],  # Corrected to use 'hovertext'
    marker=dict(size=20, color='skyblue')
)

# Create edge traces
edge_traces = []
for edge in edges:
    source_index = next(i for i, node in enumerate(nodes) if node['id'] == edge['source'])
    target_index = next(i for i, node in enumerate(nodes) if node['id'] == edge['target'])
    edge_trace = go.Scatter(
        x=[source_index, target_index, None], y=[source_index, target_index, None],
        line=dict(width=2, color='gray'),
        hoverinfo='none',
        mode='lines'
    )
    edge_traces.append(edge_trace)

# Create figure
fig = go.Figure(data=[*edge_traces, node_trace], layout=go.Layout(
    showlegend=False,
    hovermode='closest',
    margin=dict(b=20, l=5, r=5, t=40),
    xaxis=dict(showgrid=False, zeroline=False),
    yaxis=dict(showgrid=False, zeroline=False)
))

# Display the graph
st.plotly_chart(fig)
