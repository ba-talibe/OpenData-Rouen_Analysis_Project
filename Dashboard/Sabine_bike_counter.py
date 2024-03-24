from dash import Dash, html, dash_table,dcc, callback, Output, Input
import pandas as pd
import plot_functions
import data_functions
import os


# Incorporate data
# Todo : gestion de la mise à jour des données Lecture de la dernière date de téléchargement et enrichissement avec les dernières données par accès API
print(os.getcwd())
df = pd.read_csv('eco-counter-data.csv',sep=";")

data_functions.process_data(df)

app = Dash(__name__)

app.layout = html.Div([
    html.Div(children='Compteur Vélo Sabine'),
    dcc.Dropdown(data_functions.counter_list(df),data_functions.counter_list(df)[0],id="counter-dropdown"),
    dcc.Graph(figure={}, id='time-graph'),
    dcc.Graph(figure={}, id='date-graph'),
    dcc.Graph(figure={}, id='month-graph'),
    dcc.Graph(figure={}, id='year-graph'),
    dcc.Graph(figure={}, id='daily-graph'),
    dcc.Graph(figure={}, id='daily-graph-week-day'),
    dcc.Graph(figure={}, id='daily-graph-week-end'),
#    dcc.Graph(figure={}, id='daily-graph-0'),
#    dcc.Graph(figure={}, id='daily-graph-1'),
#    dcc.Graph(figure={}, id='daily-graph-2'),
#    dcc.Graph(figure={}, id='daily-graph-3'),
#    dcc.Graph(figure={}, id='daily-graph-4'),
#    dcc.Graph(figure={}, id='daily-graph-5'),
#    dcc.Graph(figure={}, id='daily-graph-6'),
    dcc.Graph(figure={}, id='weekly-graph')
    ])

# Add controls to build the interaction
@callback(
    Output(component_id='time-graph', component_property='figure'),
    Output(component_id='date-graph', component_property='figure'),
    Output(component_id='month-graph', component_property='figure'),
    Output(component_id='year-graph', component_property='figure'),
    Output(component_id='daily-graph', component_property='figure'),
    Output(component_id='daily-graph-week-day', component_property='figure'),
    Output(component_id='daily-graph-week-end', component_property='figure'),
#    Output(component_id='daily-graph-0', component_property='figure'),
#    Output(component_id='daily-graph-1', component_property='figure'),
#    Output(component_id='daily-graph-2', component_property='figure'),
#    Output(component_id='daily-graph-3', component_property='figure'),
#    Output(component_id='daily-graph-4', component_property='figure'),
#    Output(component_id='daily-graph-5', component_property='figure'),
#    Output(component_id='daily-graph-6', component_property='figure'),
    Output(component_id='weekly-graph', component_property='figure'),
    Input(component_id='counter-dropdown', component_property='value')
)
def update_graph(name):
    fig1 = plot_functions.plot_comptage_horaire(df,name)
    fig2 = plot_functions.plot_comptage_jour(df,name)
    fig3 = plot_functions.plot_comptage_mois(df,name)
    fig4 = plot_functions.plot_comptage_annee(df,name)
    fig5 = plot_functions.plot_daily(df,name)
    fig50,fig51 = plot_functions.plot_daily_week_day_week_end(df,name)
    #fig50,fig51,fig52,fig53,fig54,fig55,fig56 = plot_functions.plot_daily_by_day_of_week(df,name)
    fig6 = plot_functions.plot_weekly(df,name)
    return fig1,fig2,fig3,fig4,fig5,fig50,fig51,fig6

if __name__ == '__main__':
    app.run(debug=False)