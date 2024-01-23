import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dbdreader import DBD, MultiDBD, DBDPatternSelect
import gsw
import seawater
import cmocean
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px

data_interp = pd.read_csv("data_interp.csv")
data_interp["date_time"] = pd.to_datetime(data_interp["date_time"])
data_interp = data_interp.set_index("date_time")


app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Apply a dark theme
app.layout = html.Div(
    children=[
        dcc.Graph(id='map-plot'),
        html.Div([
            html.H6("Parâmetro:", style={'fontSize': 15, 'fontWeight': 'bold', 'color': 'white', 'fontFamily': 'Arial, sans-serif','marginBottom': 10}),  # Customize title
            dcc.Dropdown(
                id='parameter-dropdown',
                options=[
                    {'label': 'Temperatura', 'value': 'sci_water_temp_interp'},
                    {'label': 'Salinidade', 'value': 'sci_water_salinity_interp'}
                ],
                value='sci_water_temp_interp',  # Default parameter
                style={'fontSize': 12,'fontFamily': 'Arial, sans-serif', 'color': 'black', 'width': '80%'}

            ),

            html.H6("Profundidade (m):", style={'fontSize': 15, 'fontWeight': 'bold', 'color': 'white', 'fontFamily': 'Arial, sans-serif','marginBottom': 10}),  # Customize title

            dcc.Slider(
                id='depth-slider',
                min=data_interp['m_depth_interp'].min(),
                max=data_interp['m_depth_interp'].max(),
                step=1,
                marks=None,
                value=0,
                tooltip={'placement': 'bottom', 'always_visible': True},
            ),



        ],
        style={'position': 'absolute', 'top': '7%', 'left': '8%', 'width': '20%'}
        ),
    ],
    style={'backgroundColor': '#111111', 'color': '#7FDBFF'}
)

# Define callback to update the plot based on slider and dropdown values
@app.callback(
    Output('map-plot', 'figure'),
    [Input('depth-slider', 'value'),
     Input('parameter-dropdown', 'value')]
)
def update_plot(depth, selected_parameter):
    # Filter data based on the selected depth
    data_sel = data_interp[(data_interp["m_depth_interp"] >= depth - 0.1) & (data_interp["m_depth_interp"] <= depth + 0.2)].copy()

    # Create a scatter plot using Plotly Express
    fig = px.scatter_mapbox(
        data_sel,
        lat="m_lat_interp",
        lon="m_lon_interp",
        color=selected_parameter,
        color_continuous_scale=get_colormap(selected_parameter),
        range_color=[data_interp[selected_parameter].min(), data_interp[selected_parameter].max()],
        size=selected_parameter,
        size_max=10,
        opacity=0.7,
        hover_name=selected_parameter,
        hover_data=["m_depth_interp"],
        mapbox_style="white-bg",
        zoom=8.5,
        center={"lat": -23.0889, "lon": -42.3318}
    )

    # Add tile layer
    fig.update_layout(
        mapbox_layers=[
            {
                "below": 'traces',
                "sourcetype": "raster",
                "sourceattribution": 'Esri World Imagery',
                "source": [
                    'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
                ]
            }
        ],
        width=1000,
        height=700,
        title=f'Dados Comissão Ressurgência VI',
        coloraxis_colorbar=dict(
            x=0.5,
            y=0,
            len=0.5,
            outlinewidth=1,
            orientation='h',
            tickmode='array',
            tickvals=np.arange(0, 101, get_colorbar_step(selected_parameter)),
            ticktext=list(map(str, np.round(np.arange(0, 101, get_colorbar_step(selected_parameter)),2))),
            tickfont=dict(color='white'),
            thickness=10,
            title=dict(text=get_colorbar_title(selected_parameter), font=dict(color='white', size=12), side='top')
        )
    )

    return fig

def get_colormap(parameter):
    if parameter == 'sci_water_temp_interp':
        return 'thermal'
    elif parameter == 'sci_water_salinity_interp':
        return 'haline'

def get_colorbar_title(parameter):
    if parameter == 'sci_water_temp_interp':
        return 'Temperatura (°C)'
    elif parameter == 'sci_water_salinity_interp':
        return 'Salinidade (psu)'

def get_colorbar_step(parameter):
    if parameter == 'sci_water_temp_interp':
        return 2
    elif parameter == 'sci_water_salinity_interp':
        return 0.2

def save_html(fig):
    return dcc.Store(id='figure-store', data=fig.to_dict())

html_output = html.Div(id='html-output')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_ui=False, dev_tools_props_check=False, port=8051)
