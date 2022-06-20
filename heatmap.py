import folium
from folium import plugins


# Function to convert the dataframe into geo latlng for parse to map
def parse_latlng(data):
    latlng = []

    for row in data:
        latlng_index = []
        latlng_index.append(row["Geo"]["lat"])
        latlng_index.append(row["Geo"]["lng"])
        latlng.append(latlng_index)
    return latlng


# Heatmap function
def heatmap(data):
    latlng = parse_latlng(data)
    map = folium.Map([-30.0493292, -51.1964431], zoom_start=13)
    map.add_child(plugins.HeatMap(latlng, radius=17))
    return map._repr_html_()
