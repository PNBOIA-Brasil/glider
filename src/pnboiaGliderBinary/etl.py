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
from dotenv import load_dotenv

load_dotenv()




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
        return (self.db
                .get(table="data.parameters", type=["=", parameter_type])
                .sort_values("id")
                )

    # def get_system_data
