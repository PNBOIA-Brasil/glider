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
        self.extension = extension.strip(".")

    # WIDE DATAFRAME METHODS
    def generate_wide_dataframe(self, parameters_type:str="eng"):
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

    def merge_sci_eng(self, science_data:pd.DataFrame, engineering_data:pd.DataFrame):
        print(f"Merging eng and sci data to a single dataframe...")
        return pd.merge(science_data, engineering_data, on="time", how="outer")

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

    def compose_data_file_name(self, file_type:str="narrow"):
        print("Composing filename...")
        return f"{self.glider_unit_name}_{self.mission_params_first}_to_{self.mission_params_last}_{self.extension}_{file_type}.csv"

    def save_csv_file(self, data:pd.DataFrame, file_type:str="narrow"):
        file_name = self.compose_data_file_name(file_type=file_type)
        print(f"Saving file as {file_name}...")
        file_path = os.path.join(self.binary_files_path, file_name)
        data.to_csv(file_path)

    # NARROW DATAFRAME METHODS
    def generate_narrow_dataframe(self, parameters_type:str="eng"):

        data = pd.DataFrame(columns=["time", "variable", "value"])

        if hasattr(self,"bd"):
            for parameter in self.bd.parameterNames[parameters_type]:
                time, values = self.bd.get(parameter)
                single_param_data = pd.DataFrame({"time":time, "variable":parameter, "value":values})
                if data.empty:
                    data = single_param_data
                else:
                    data = pd.concat([data, single_param_data], axis=0)

        else:
            raise AttributeError("No binary data attribute was created. Please, review your instantiation using the MultiDBD tool.")

        return data

    def round_values(self, data:pd.DataFrame, round_number:int=4):
        print(f"Rouding values by {round_number}...")
        data["value"] = data["value"].round(round_number)
        return data

    def create_data_type_column(self, data:pd.DataFrame, data_type:str="engineering"):
        print(f"Creating data_type ({data_type}) column...")
        data.insert(1,"data_type", data_type)
        return data

    def concat_sci_eng(self, science_data:pd.DataFrame, engineering_data:pd.DataFrame):
        return pd.concat([science_data, engineering_data], axis=0)

    def drop_redundant_parameters(self, science_data:pd.DataFrame, engineering_data:pd.DataFrame):
        test = np.isin(engineering_data.values, science_data.values)
        idxs = [idx for idx, t in enumerate(test) if t]

        pass

    def pivot_data(self, data:pd.DataFrame):
        data = data.reset_index()
        return data.pivot(index="date_time", columns="variable", values="value")


if __name__ == "__main__":
    print("="*30)
    print("RUNNING BINARY DATA PROCESSOR")

    if len(sys.argv) < 1:
        raise AttributeError("Please, provide the path to the files directory.")

    if sys.argv[2] == "small":
        extension = ".[st]bd"
    elif sys.argv[2] == "big":
        extension = ".[de]bd"

    g = GliderData(binary_files_path=sys.argv[1], cache_dir=sys.argv[1], extension=extension)

    # decode binary data
    g.bd = MultiDBD(pattern=g.pattern, cacheDir=g.cache_dir)


    # process data
    g.science_data = g.generate_narrow_dataframe(parameters_type="sci")
    g.science_data = g.create_data_type_column(data=g.science_data, data_type="science")
    g.engineering_data = g.generate_narrow_dataframe(parameters_type="eng")
    g.engineering_data = g.create_data_type_column(data=g.engineering_data, data_type="engineering")

    g.all_data = g.concat_sci_eng(science_data=g.science_data, engineering_data=g.engineering_data)

    g.all_data = g.round_values(data=g.all_data, round_number=4)

    g.all_data["date_time"] = g.convert_to_datetime(time=g.all_data["time"])
    g.all_data = g.all_data.set_index("date_time").sort_index()

    # g.all_data_wide = g.pivot_data(data=g.all_data)

    # save data
    g.save_csv_file(data=g.all_data, file_type="narrow")
    # g.save_csv_file(data=g.all_data_wide, file_type="wide")


    # g.science_data = g.generate_dataframe(parameters_type="sci")
    # g.engineering_data = g.generate_dataframe(parameters_type="eng")
    # g.all_data = g.merge_sci_eng(science_data=g.science_data, engineering_data=g.engineering_data)

    # g.all_data["date_time"] = g.convert_to_datetime(time=g.all_data["time"])
    # g.all_data = g.all_data.set_index("date_time").sort_index()

    # # save data
    # g.save_csv_file(data=g.all_data)

    print("\nSUCCESSFULL PROCESSING")
