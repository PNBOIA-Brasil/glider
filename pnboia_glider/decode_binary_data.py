"""
PNBoia Glider Binary Data Processor
Author: Thiago Caminha
version: 0.0.1

A script to generate a csv file from the glider binary and cache data.

Credits:
Lucas Merckelbach (https://github.com/smerckel), author of the dbdreder package.
"""

from dbdreader import DBD, MultiDBD, DBDPatternSelect
from datetime import datetime
import pandas as pd
import numpy as np
import os
from glob import glob
import re
import sys

class GliderData():

    def __init__(self, binary_files_path:str, cache_dir:str, extension:str=".[st]bd"):

        self.binary_files_path = binary_files_path
        self.extension = "*" + extension
        self.pattern = os.path.join(binary_files_path,self.extension)
        self.cache_dir = cache_dir

        self.data_file_names = glob(os.path.join(binary_files_path,"*bd"))
        self.cache_file_names = glob(os.path.join(cache_dir,"*.cac"))

        self.glider_unit_name = self.extract_section_from_data_file_name(data_file_names=self.data_file_names,
                                                                            section="glider_unit_name",
                                                                            index=0)
        self.mission_params_first = self.extract_section_from_data_file_name(data_file_names=self.data_file_names,
                                                                            section="mission_params",
                                                                            index=0)
        self.mission_params_last = self.extract_section_from_data_file_name(data_file_names=self.data_file_names,
                                                                            section="mission_params",
                                                                            index=-1)


    def generate_dataframe(self, parameters_type:str="eng"):
        print(f"Generating {parameters_type} dataframe...")

        data = pd.DataFrame([])

        if hasattr(self,"bd"):
            for parameter in self.bd.parameterNames[parameters_type]:
                tm, param = self.bd.get(parameter)
                df = pd.DataFrame({"time":tm, parameter:param})
                # print(parameter, "shape:", param.shape)
                if data.empty:
                    data = df
                else:
                    data = pd.merge(left=data, right=df, on="time", how="outer")
        else:
            raise AttributeError("No binary data attribute was created. Please, review your instantiation using the MultiDBD tool.")


        return data

    def sort_by_time(self, data:pd.DataFrame):
        return data.sort_values("time")

    def merge_sci_eng(self, science_data:pd.DataFrame, engineer_data:pd.DataFrame):
        print(f"Merging eng and sci data to a single dataframe...")
        return pd.merge(science_data, engineer_data, on="time", how="outer")

    def convert_to_datetime(self, time:np.array):
        print("Converting timestamp to datetime...")
        date_time = pd.to_datetime(time, unit="s")
        return date_time

    def extract_section_from_data_file_name(self, data_file_names:list, section:str, index:int):

        if section == "glider_unit_name":
            pattern = r'unit_\d+'
        elif section == "mission_params":
            pattern = r'(\d{4}-\d{3}-\d-\d)'

        match = re.search(pattern, data_file_names[index])

        return match.group(0)

    def compose_data_file_name(self):
        print("Composing filename...")
        return f"{self.glider_unit_name}_{self.mission_params_first}_to_{self.mission_params_last}.csv"

    def save_csv_file(self, data:pd.DataFrame):
        file_name = self.compose_data_file_name()
        print(f"Saving file as {file_name}...")
        file_path = os.path.join(self.binary_files_path, file_name)
        data.to_csv(file_path)




if __name__ == "__main__":
    print("="*30)
    print("RUNNING BINARY DATA PROCESSOR")

    if len(sys.argv) < 1:
        raise AttributeError("Please, provide the path to the files directory.")

    if sys.argv[2] == "small":
        extension = ".[st]bd"
    elif sys.argv[2] == "big":
        extension = ".[de]bd"
    print(sys.argv[1])
    print(sys.argv[2])

    g = GliderData(binary_files_path=sys.argv[1], cache_dir=sys.argv[1], extension=extension)

    # decode binary data
    g.bd = MultiDBD(pattern=g.pattern, cacheDir=g.cache_dir)

    # process data
    g.science_data = g.generate_dataframe(parameters_type="sci")
    g.engineer_data = g.generate_dataframe(parameters_type="eng")
    g.all_data = g.merge_sci_eng(science_data=g.science_data, engineer_data=g.engineer_data)

    g.all_data["date_time"] = g.convert_to_datetime(time=g.all_data["time"])
    g.all_data = g.all_data.set_index("date_time").sort_index()

    # save data
    g.save_csv_file(data=g.all_data)

    print("\nSUCCESSFULL PROCESSING")
