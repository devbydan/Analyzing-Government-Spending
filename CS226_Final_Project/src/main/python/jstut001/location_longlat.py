from geopy.geocoders import Nominatim
import pandas as pd
import numpy as np
import os

def getLat(g):
	if g is None:
		return None
	return g.latitude

def getLong(g):
	if g is None:
		return None
	return g.longitude

geolocator = Nominatim(timeout = 10, user_agent="jstut001")

def geoFix(city, state, oldGeo):
	if not oldGeo is None:
		return oldGeo
	return geolocator.geocode(city + "," + state)

locations = pd.read_csv("../../../../Locations.csv/part-00000-a3d7ac2e-389b-497c-924d-d3dbe3dad872-c000.csv")
locations['ADDRESS'] = locations.R_ADDR + ", " + locations.R_CITY + ", " + locations.R_STATE
locations['GEO'] = locations.ADDRESS.apply(geolocator.geocode)
locations['GEO'] = [geoFix(row['R_STATE'], row['R_CITY'], row['GEO']) for index, row in locations.iterrows()]
locations['lat'] = [getLat(g) for g in locations.GEO]
locations['long'] = [getLong(g) for g in locations.GEO]
locations.to_csv('../../../../LocationsMap.csv')