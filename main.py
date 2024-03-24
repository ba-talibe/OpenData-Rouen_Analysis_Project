from dash import Dash, html, dash_table,dcc, callback, Output, Input
import pandas as pd
import Dashboard.plot_functions
import Dashboard.data_functions
import os
from data_toolset.data_loading import update_dataset, load_dataset


print(os.getcwd())

# update and load dataset
update_dataset(os.path.join("data_toolset"))
df = load_dataset(dataset_path)


app = Dash(__name__)