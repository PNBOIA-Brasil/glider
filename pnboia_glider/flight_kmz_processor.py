import pandas as pd
import numpy as np

import folium
from folium.plugins import MeasureControl

import glob
import zipfile
from bs4 import BeautifulSoup
import re
import os
import sys

import webbrowser

import warnings
warnings.filterwarnings("ignore")


class KMZParser:
    def __init__(self, folder_path:str):

        # file handling
        self.output_interactive_map_html_file_name = "flight_map.html"
        self.output_surfacings_csv_file_name = "surfacings.csv"
        self.output_surface_movements_csv_file_name = "surface_movements.csv"
        self.output_glider_tracks_csv_file_name = "glider_tracks.csv"
        self.output_depth_curr_csv_file_name = "depth_avg_currents.csv"


        self.kmz_file_name = self.grab_kmz_file(folder_path=folder_path)

        self.kml = self.convert_to_kml(filepath=self.kmz_file_name)
        self.soup = self.parse_kml_as_soup(kml=self.kml)
        self.folders = self.parse_folders(soup=self.soup)
        self.folders_names = self.parse_all_folders_names(folders=self.folders)

        # strings handling
        self._gps_time_string_pattern = r"Time of GPS Position: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
        self._glider_track_range_string_pattern = r"Range: ([-+]?\d*\.\d+|\d+|NaN)[A-Za-z/]+.*?Speed: ([-+]?\d*\.\d+|\d+|NaN)[A-Za-z/]+ @ (\d)"
        self._depth_current_avg_string_pattern = r"Speed: ([-+]?\d*\.\d+|\d+|NaN)[A-Za-z/]+ @ (\d)"


        # surfacings coords
        self.surfacings_coords = self.parse_surfacings_coordinates(folders=self.folders)
        self.surfacings_coords_cols_names = ["folder_name", "gps_date_time", "longitude", "latitude" ]
        self.surfacings_coords_df = self.generate_coordinates_dataframe(coordinates=self.surfacings_coords, columns_names=self.surfacings_coords_cols_names)

        # surface movements coords
        self.surface_movements_coords_cols_names = ["folder_name", "gps_date_time", "longitude", "latitude" ]
        self.surface_movements_coords = self.parse_surface_movements_coordinates(folders=self.folders)
        self.surface_movements_coords_df = self.generate_coordinates_dataframe(coordinates=self.surface_movements_coords,
                                                                    columns_names=self.surface_movements_coords_cols_names)

        # glider track
        self.glider_track_coords_cols_names = ["folder_name", "range", "speed", "degree","start_longitude", "start_latitude", "end_longitude", "end_latitude"]
        self.glider_track_coords = self.parse_glider_tracks_coordinates(folders=self.folders)
        self.glider_track_coords_df = self.generate_coordinates_dataframe(coordinates=self.glider_track_coords,
                                                                    columns_names=self.glider_track_coords_cols_names)


        # depth current avg vectors
        self.depth_current_avg_coords_cols_names = ["folder_name", "speed", "degree","start_longitude", "start_latitude", "end_longitude", "end_latitude"]
        self.depth_current_avg_coords = self.parse_depth_current_coordinates(folders=self.folders)
        self.depth_current_avg_coords_df = self.generate_coordinates_dataframe(coordinates=self.depth_current_avg_coords,
                                                                    columns_names=self.depth_current_avg_coords_cols_names)


        # interactive map
        self.interactive_map = self.plot_map(surfacings_data=self.surfacings_coords_df,
                surface_movements_data=self.surface_movements_coords_df,
                glider_tracks_data=self.glider_track_coords_df,
                depth_avg_currents_data=self.depth_current_avg_coords_df)

        self.save_map_as_html(map=self.interactive_map, file_name=self.output_interactive_map_html_file_name)

    def grab_kmz_file(self, folder_path:str):
        folder_path = os.path.join(folder_path, "*.kmz")
        files = glob.glob(folder_path, recursive=False)
        return files[0]

    def convert_to_kml(self, filepath:str):
        print("Converting file to kml...")
        with zipfile.ZipFile(filepath, 'r') as kmz:
            return kmz.open(kmz.filelist[0].filename, 'r').read()

    def parse_kml_as_soup(self, kml:bytes):
        print("Parsing kml...")
        return BeautifulSoup(kml, 'html.parser')

    def parse_folders(self, soup:BeautifulSoup):
        print("Parsing kml folders...")
        return soup.find_all("folder")

    def parse_all_folders_names(self, folders):
        print("Extracting all folders names...")
        folders_names = []
        for folder in self.folders:
            folders_names.append(folder.find("name").text)
        return folders_names

    def get_folder_index(self, folder_name:str):
        return self.folders_names.index(folder_name)

    def generate_coordinates_dataframe(self, coordinates:list, columns_names:list):
        return pd.DataFrame(columns=columns_names, data=coordinates)

    def parse_find_all(self, folder, child_name:str):
        parsed = folder.find_all(child_name)
        if not parsed:
            raise ValueError(f"No matches for {child_name} found. Aborting parsing.")
        return parsed

    def parse_find(self, parent, child_name:str):
        parsed = parent.find(child_name)
        if not parsed:
            raise ValueError(f"No matches for {child_name} found. Aborting parsing.")
        return parsed

    def parse_surfacings_coordinates(self, folders, folder_name:str="Surfacings"):
        print(f"Parsing {folder_name} folder...")
        index = self.get_folder_index(folder_name=folder_name)
        folder_name = self.folders_names[index]
        folder = self.folders[index]

        coordinates = []

        placemarks = self.parse_find_all(folder=folder, child_name="placemark")

        for placemark in placemarks:
            coordinates_text = self.parse_find(parent=placemark, child_name="coordinates").text
            try:
                gps_time_text = placemark.find("description").text
                gps_time = re.search(self._gps_time_string_pattern, gps_time_text).group(1)
            except:
                gps_time = np.nan

            coordinates_list = [tuple(map(float, coord.split(","))) for coord in coordinates_text.strip().split()]

            for coord in coordinates_list:
                coordinates.append((folder_name, gps_time, coord[0], coord[1]))

        return coordinates

    def parse_surface_movements_coordinates(self, folders, folder_name:str="Surface Movements"):
        print(f"Parsing {folder_name} folder...")

        index = self.get_folder_index(folder_name=folder_name)
        folder_name = self.folders_names[index]
        folder = self.folders[index]

        coordinates = []

        placemarks = self.parse_find_all(folder=folder, child_name="placemark")

        for placemark in placemarks:
            coordinates_text = self.parse_find(parent=placemark, child_name="coordinates").text
            try:
                gps_time_text = placemark.find("description").text
                gps_time = re.search(self._gps_time_string_pattern, gps_time_text).group(1)
            except:
                gps_time = np.nan

            coordinates_list = [tuple(map(float, coord.split(","))) for coord in coordinates_text.strip().split()]

            for coord in coordinates_list:
                coordinates.append((folder_name, gps_time, coord[0], coord[1]))

        return coordinates

    def parse_glider_tracks_coordinates(self, folders, folder_name:str="Glider Tracks"):
        print(f"Parsing {folder_name} folder...")

        index = self.get_folder_index(folder_name=folder_name)
        folder_name = self.folders_names[index]
        folder = self.folders[index]

        coordinates = []

        placemarks = self.parse_find_all(folder=folder, child_name="placemark")

        for placemark in placemarks:
            coordinates_text = self.parse_find(parent=placemark, child_name="coordinates").text
            track_info_text = placemark.find("description").text
            track_info = re.search(self._glider_track_range_string_pattern, track_info_text)
            range = track_info.group(1)
            speed = track_info.group(2)
            deg = track_info.group(3)

            coordinates_list = [tuple(map(float, coord.split(","))) for coord in coordinates_text.strip().split()]

            coordinates.append((folder_name,
                            range,
                            speed,
                            deg,
                            coordinates_list[0][0],
                            coordinates_list[0][1],
                            coordinates_list[1][0],
                            coordinates_list[1][1]))

        return coordinates

    def parse_depth_current_coordinates(self, folders, folder_name:str="Depth Averaged Current Vectors"):
        print(f"Parsing {folder_name} folder...")

        index = self.get_folder_index(folder_name=folder_name)
        folder_name = self.folders_names[index]
        folder = self.folders[index]

        coordinates = []

        placemarks = self.parse_find_all(folder=folder, child_name="placemark")

        for placemark in placemarks:
            coordinates_text = self.parse_find(parent=placemark, child_name="coordinates").text
            track_info_text = placemark.find("description").text
            track_info = re.search(self._depth_current_avg_string_pattern, track_info_text)
            speed = track_info.group(1)
            deg = track_info.group(2)

            coordinates_list = [tuple(map(float, coord.split(","))) for coord in coordinates_text.strip().split()]

            coordinates.append((folder_name,
                            speed,
                            deg,
                            coordinates_list[0][0],
                            coordinates_list[0][1],
                            coordinates_list[1][0],
                            coordinates_list[1][1]))

        return coordinates

    def plot_map(self,
                surfacings_data:pd.DataFrame,
                surface_movements_data:pd.DataFrame,
                glider_tracks_data:pd.DataFrame,
                depth_avg_currents_data:pd.DataFrame,
                zoom_start=10,
                center=None):
        print("Generating interactive map...")
        # map_center = [data['latitude'].iloc[0], data['longitude'].iloc[0]]
        map = folium.Map(zoom_start=zoom_start, control_scale=True, location=(-22.92830339525606, -43.137900250593106))

        tile_layer = folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Esri World Imagery',
            name='Bathymetry',
            overlay=True,
            control=True
        ).add_to(map)

        title_html = """
            <div style="
                position: absolute;
                top: 10px;
                left: 60px;
                background-color: rgba(255, 255, 255, 0.8);
                padding: 10px;
                border-radius: 5px;
                z-index: 1000;
                display: flex;
                align-items: center;">
                <img src="https://i.imgur.com/rJ4KKmn.png" alt="Imgur Image" style="
                    width: 232.5px;
                    height: 75px;
                    margin-right: 10px;
                    border-radius: 5px;">
                <div style="
                    display: flex;
                    flex-direction: column;
                    align-items: flex-start;">
                    <h3 style="margin: 0;">Glider Flight Data</h3>
                    <p style="margin: 0;">Interactive Map</p>
                </div>
            </div>
        """

        map.get_root().html.add_child(folium.Element(title_html))


        surfacings_layer = folium.FeatureGroup(name='Surfacings', overlay=True).add_to(map)
        surface_movements_layer = folium.FeatureGroup(name='Surface Movements', overlay=True).add_to(map)
        glider_tracks_layer = folium.FeatureGroup(name='Glider Tracks', overlay=True).add_to(map)
        depth_currents = folium.FeatureGroup(name='Depth Avg Currents', overlay=True).add_to(map)


        for idx, row in surfacings_data.iterrows():
            coord = [row['latitude'],row['longitude']]
            # marker = folium.CircleMarker(coord,
            #                     radius=3,
            #                     fill_color='cornflowerblue',
            #                     color=None,
            #                     fill_opacity=1,
            #                     fill=True,z_index=1000).add_to(surfacings_layer)

            text = f'''<div style="font-family: sans-serif; font-size: 12px;">
                <b>unit_1094</b><br>
                <b>Datahora:</b> {row.gps_date_time}Z<br>
                <b>Latitude:</b> {round(row['latitude'],6)}<br>
                <b>Longitude:</b> {round(row['longitude'], 6)}
            </div>'''

            iframe = folium.IFrame(text, width=220, height=75)
            popup = folium.Popup(iframe, max_width=300, popup_class='folium.features.LatLngPopup')

            icon_image="https://i.imgur.com/BJqEyd0.png"
            icon_size=(25,20)
            marker = folium.Marker(coord,
                                popup=popup,
                                icon=folium.CustomIcon(icon_image=icon_image, icon_size=icon_size)
                                ).add_to(surfacings_layer)

            if idx < len(surfacings_data) - 1:
                next_location = [surfacings_data['latitude'].iloc[idx + 1], surfacings_data['longitude'].iloc[idx + 1]]
                lines = folium.PolyLine(locations=[coord, next_location],
                                        color='white',
                                        dash_array='4, 4',
                                        weight=1,z_index=1000).add_to(surfacings_layer)



        for idx, row in surface_movements_data.iterrows():
            coord = [row['latitude'],row['longitude']]
            # marker = folium.CircleMarker(coord,
            #                     radius=3,
            #                     fill_color='red',
            #                     color=None,
            #                     fill_opacity=1,
            #                     fill=True,z_index=1000).add_to(surface_movements_layer)

            text = f'''<div style="font-family: sans-serif; font-size: 12px;">
                <b>unit_1094</b><br>
                <b>Datahora:</b> {row.gps_date_time}Z<br>
                <b>Latitude:</b> {round(row['latitude'],6)}<br>
                <b>Longitude:</b> {round(row['longitude'], 6)}
            </div>'''

            iframe = folium.IFrame(text, width=220, height=75)
            popup = folium.Popup(iframe, max_width=300, popup_class='folium.features.LatLngPopup')


            icon_image="https://i.imgur.com/BJqEyd0.png"
            icon_size=(25,20)
            marker = folium.Marker(coord,
                                popup=popup,
                                icon=folium.CustomIcon(icon_image=icon_image, icon_size=icon_size)
                                ).add_to(surface_movements_layer)

            # if idx < len(surface_movements_data) - 1:
            #     next_location = [surface_movements_data['latitude'].iloc[idx + 1], surface_movements_data['longitude'].iloc[idx + 1]]
            #     lines = folium.PolyLine(locations=[coord, next_location],
            #                             color='white',
            #                             dash_array='4, 4',
            #                             weight=1,z_index=1000).add_to(surface_movements_layer)


        for idx, row in glider_tracks_data.iterrows():
            lines = folium.PolyLine(locations=[[row["start_latitude"], row["start_longitude"]], [row["end_latitude"],row["end_longitude"]]],
                                        color='white',
                                        dash_array='4, 4',
                                        weight=1,z_index=1000).add_to(glider_tracks_layer)

        for idx, row in depth_avg_currents_data.iterrows():
            lines = folium.PolyLine(locations=[[row["start_latitude"], row["start_longitude"]], [row["end_latitude"],row["end_longitude"]]],
                                        color='green',
                                        weight=3,z_index=1000).add_to(depth_currents)
        # surfacings_layer = folium.FeatureGroup(name='Surfacings', overlay=True).add_to(map)
        folium.LayerControl().add_to(map)

        MeasureControl(primary_length_unit='meters',
                        primary_area_unit='sqmeters').add_to(map)

        return map

    def save_map_as_html(self, map, file_name:str):
        print(f"Saving interactive map as {file_name}")
        map.save(os.path.join("htmls/", file_name))

    def open_interactive_map_in_webbrowser(self, html_file_path):
        webbrowser.open(os.path.join("file://", html_file_path), new=2)


if __name__ == "__main__":
    print("="*30)
    print("RUNNING GLIDER FLIGHT DATA PROCESSOR")

    k = KMZParser(folder_path="data/")


    print(f"Saving Surfacings data as {k.output_surfacings_csv_file_name}")
    k.surfacings_coords_df.to_csv(os.path.join("data/",k.output_surfacings_csv_file_name))
    print(f"Saving Surface Movements data as {k.output_surface_movements_csv_file_name}")
    k.surface_movements_coords_df.to_csv(os.path.join("data/",k.output_surface_movements_csv_file_name))
    print(f"Saving Glider Tracks data as {k.output_glider_tracks_csv_file_name}")
    k.glider_track_coords_df.to_csv(os.path.join("data/",k.output_glider_tracks_csv_file_name))
    print(f"Saving Depth Avarage Currents data as {k.output_depth_curr_csv_file_name}")
    k.depth_current_avg_coords_df.to_csv(os.path.join("data/",k.output_depth_curr_csv_file_name))

    print("\nSUCCESSFULL PROCESSING")

    if len(sys.argv) > 1:
        if sys.argv[1] in ("-oim", "--open-interactive-map"):
            print("\nOpenning interactive map in your default webbrowser...")
            html_file_path = os.path.join(os.getcwd(),"htmls/", k.output_interactive_map_html_file_name)
            k.open_interactive_map_in_webbrowser(html_file_path=html_file_path)
