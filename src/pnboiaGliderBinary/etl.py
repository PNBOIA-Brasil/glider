"""
PNBoia Glider Binary Data Processor
Author: Thiago Caminha
version: 0.0.1

A script that uses the dbdreader package to decode and generate csv file from the glider binary and cache data.

Credits:
Lucas Merckelbach (https://github.com/smerckel), author of the dbdreder package.
"""

from dbdreader import MultiDBD
from datetime import datetime
import pandas as pd
import numpy as np
import os
from glob import glob
import re
import sys
from pnboiaGliderDataBase.db import GetData


class PNBOIAGlider():

    def __init__(self, mission_id:int=None, mission_name:str=None, conn=None):

        if not any([mission_id,mission_name]):
            raise AttributeError("Please provide either a mission id or mission name.")
        self.mission_id = mission_id
        self.mission_name = mission_name

        if conn:
            self.db = GetData(conn=conn)
        else:
            self.db = GetData(host=os.getenv('PNBOIA_GLIDER_HOST'),
                                database=os.getenv('PNBOIA_GLIDER_DB'),
                                user=os.getenv('PNBOIA_GLIDER_USER'),
                                password=os.getenv('PNBOIA_GLIDER_PSW'))

    def get_mission_info(self, mission_id):
        return (self.db
                .get(table="glider.missions", mission_id=["=", mission_id])
            )

    def compose_multidbd_pattern(self, binary_files_path:str, extension:str):
        extension = "*" + extension
        return os.path.join(binary_files_path, extension)

    def decode_binary_data(self, pattern:str, cache_dir:str):
        return MultiDBD(pattern=pattern, cacheDir=cache_dir)

    def get_parameters(self, parameter_type:str):
        print(f"\nGrabing {parameter_type} parameters")
        return (self.db
                .get(table="data.parameters", type=["=", parameter_type])
                .sort_values("id")
                )

    def generate_narrow_dataframe(self, parameters:pd.DataFrame):

        data = pd.DataFrame(columns=["time", "parameter_id", "value"])

        if hasattr(self,"bd"):
            for idx, row in parameters[['id','name']].iterrows():
                print(f"Grabing {row['name']} (parameter_id = {row.id})")
                time, values = self.bd.get(row['name'])
                single_param_data = pd.DataFrame({"time":time,
                                                    "parameter_id": row.id,
                                                    "value":values})
                if data.empty:
                    data = single_param_data
                else:
                    data = pd.concat([data, single_param_data], axis=0)

        else:
            raise AttributeError("No binary data attribute was created. Please, review your instantiation using the MultiDBD tool.")

        return data

    def concat_sci_eng(self, science_data:pd.DataFrame, engineering_data:pd.DataFrame):
        return pd.concat([science_data, engineering_data], axis=0)

    def convert_to_datetime(self, data:pd.DataFrame):
        print("Converting timestamp to datetime")
        date_time = pd.to_datetime(data['time'] , unit="s")
        data['date_time'] = date_time
        data = data.drop(columns="time")
        return data

    def round_values(self, data:pd.DataFrame, round_number:int=4):
        print(f"Rouding values by {round_number}")
        data["value"] = data["value"].round(round_number)
        return data

    def insert_mission_id(self, data:pd.DataFrame, mission_id:int):
        print(f"Inserting mission_id ({mission_id})")
        data["mission_id"] = mission_id
        return data
