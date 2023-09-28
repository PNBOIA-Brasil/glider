import pandas as pd
import plotly.graph_objs as go
from datetime import datetime, timedelta
import glob
import webbrowser
import os
import sys

class SFMCGliderData():
    """
    A tool for reading and processing manually extracted glider data from the SFMC.

    """

    def __init__(self, folder_path:str):
        self.output_html_file_name = "glider_sci_data_timeseries.html"
        self.output_csv_file_name = "glider_sci_data_timeseries.csv"


        self.raw_data = self.load_all_files(folder_path=folder_path)
        self.units = self.get_units(data=self.raw_data)

        self.sci_data = self.process_data(data=self.raw_data)
        self.timeseries = self.plot_timeseries(data=self.sci_data)
        self.save_plot_as_html(plot=self.timeseries, file_name=self.output_html_file_name)

    def grab_txt_files(self, folder_path:str):
        folder_path = os.path.join(folder_path, "*.txt")
        return glob.glob(folder_path, recursive=False)

    def load_individual_file(self, filepath:str, sep:str=","):
        return pd.read_csv(filepath, sep=sep)

    def load_all_files(self, folder_path:str):
        print("\nLoading the files:")
        files = self.grab_txt_files(folder_path=folder_path)
        for file in files:
            print("-", file)
            if 'global_data' not in locals():
                global_data = self.load_individual_file(file, sep=" ")
            else:
                data = self.load_individual_file(file, sep=" ")
                global_data = self.merge_sci_data(data1=global_data, data2=data)

        # global_data = global_data[global_data.columns.sort_values()]
        if 'global_data' not in locals():
            raise Exception("Unnable to load data files. Make sure the correct path is being passed.")
        else:
            return global_data


    def convert_data_types(self, data:pd.DataFrame):
        data_types= {"time": int,
                     "m_depth": float,
                     "sci_seaowl_fdom_scaled": float,
                     "sci_rbrctd_salinity_00": float,
                     "sci_oxy4_oxygen": float,
                     "sci_rbrctd_pressure_00": float,
                     "sci_seaowl_chl_sig": float,
                     "sci_rbrctd_temperature_00": float,
                     "sci_oxy4_saturation": float,
                     "sci_rbrctd_conductivity_00": float
                }
        for column in data_types:
            if column in data.columns:
                data[column] = data[column].astype(data_types[column])

        return data


    def drop_unwanted_columns(self, data:pd.DataFrame):
        columns_to_drop = data.filter(regex="Unnamed").columns
        return data.drop(columns=columns_to_drop)

    def get_units(self, data:pd.DataFrame):
        params = data.columns
        units = data.iloc[0].values
        units_params = dict(zip(params,units))
        self.units_params = units_params
        return units_params

    def drop_units_row(self, data:pd.DataFrame):
        return data.drop(index=0)

    def timestamp_to_datetime(self, timestamp:pd.Series):
        return timestamp.apply(lambda x : datetime.fromtimestamp(x))

    def rename_columns(self):
        pass

    def merge_sci_data(self, data1:pd.DataFrame, data2:pd.DataFrame):
        return pd.merge(data1, data2.drop(columns=["m_depth"]), on="time", how="outer")

    def process_data(self, data:pd.DataFrame):
        print("\nProcessing the data...")
        data = self.drop_unwanted_columns(data=data)
        data = self.drop_units_row(data=data)

        data = self.convert_data_types(data=data)
        date_time = self.timestamp_to_datetime(timestamp=data['time'])
        data.insert(0, column="date_time", value=date_time)
        data = data.set_index("date_time").sort_index(ascending=False)

        return data

    def plot_timeseries(self, data:pd.DataFrame):
        traces = []
        parameters = data.drop(columns="time").columns

        for parameter in parameters:
            trace = go.Scatter(x=data.index, y=data[parameter],
                                mode='lines+markers', name=parameter)
            traces.append(trace)

        layout = go.Layout(
            xaxis=dict(
                rangeslider=dict(visible=True),
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1 mês", step="month", stepmode="backward"),
                        dict(count=6, label="6 meses", step="month", stepmode="backward"),
                        dict(count=1, label="1 ano", step="year", stepmode="backward"),
                        dict(count=1, label="Início do ano", step="year", stepmode="todate"),
                        dict(label="Todo o período", step="all")
                    ])
                ),
                title="Datahora",
                title_font=dict(size=14),
                showgrid=True,
                gridcolor="lightgrey",
                range=[data.index[0] - timedelta(days=30), data.index[0] + timedelta(hours=12)]
            ),
            yaxis=dict(
                title_font=dict(size=14),
                showgrid=True,
                gridcolor="lightgrey")
            # ),
            # updatemenus=[
            #     {
            #         'buttons': [
            #             {'label': parameter, 'method': 'update', 'args': [{'visible': [i == j for i in range(len(parameters))]}]} for j, parameter in enumerate(parameters)
            #         ],
            #         'direction': 'down',
            #         'showactive': True,
            #         'x': 0.1,
            #         'xanchor': 'left',
            #         'y': 1.5,
            #         'yanchor': 'top',
            #     }
            # ],
            # font={
            #     'family': 'Arial',
            #     'size': 15,
            # },
        )

        fig = go.Figure(data=traces, layout=layout)

        fig.update_layout(
            margin=dict(r=40, t=100),
        )

        return fig

    def save_plot_as_html(self, plot, file_name:str):
        print(f"\nSaving plot as {file_name}")
        plot.write_html(os.path.join("htmls/", file_name))

    def open_timeseries_in_webbrowser(self, html_file_path):
        webbrowser.open(os.path.join("file://", html_file_path), new=2)


if __name__ == "__main__":
    print("="*30)
    print("RUNNING GLIDER SCI DATA PROCESSOR")

    gd = SFMCGliderData(folder_path="data/")

    print(f"\nSaving data as {gd.output_csv_file_name}")
    gd.sci_data.to_csv(os.path.join("data/",gd.output_csv_file_name))

    print("\nSUCCESSFULL PROCESSING")

    if len(sys.argv) > 1:
        if sys.argv[1] in ("-ots", "--open-timeseries"):
            print("\nOpenning timeseries in your default webbrowser...")
            html_file_path = os.path.join(os.getcwd(),"htmls/", gd.output_html_file_name)
            gd.open_timeseries_in_webbrowser(html_file_path=html_file_path)
