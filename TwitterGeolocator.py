from geopy.geocoders import Nominatim
import re

def remove_unicode(string_with_nonascii):
    if isinstance(string_with_nonascii, str):
        encoded_string = string_with_nonascii.encode("ascii", "ignore")
        string_with_nonascii = encoded_string.decode()
    return string_with_nonascii


def determine_loc(coord_or_place=None):
    coord_or_place = remove_unicode(coord_or_place)
    geolocator = Nominatim(user_agent="https://maps.googleapis.com")
    try:
        if isinstance(coord_or_place, str):
            location = geolocator.geocode(coord_or_place, exactly_one=True)
            latitude = location.raw['lat']
            longitude = location.raw['lon']
            coord_or_place = [latitude, longitude]
        location = geolocator.reverse(coord_or_place, exactly_one=True)
        address = location.raw['address']
        city = remove_unicode(address.get('city', ''))
        city = re.sub('[^a-zA-Z \n\.]', '', city).title().strip()
        state = remove_unicode(address.get('state', ''))
        state = re.sub('[^a-zA-Z \n\.]', '', state).title().strip()
        country = remove_unicode(address.get('country', ''))
        country = re.sub('[^a-zA-Z \n\.]', '', country).title().strip()
        postcode = remove_unicode(address.get('postcode', ''))
        postcode = re.sub('[^a-zA-Z0-9 \n\.]', '', postcode).strip()
        return country, state, city, postcode
    except Exception as ex:
        print(f"Exception occurred while trying to locate: {coord_or_place}. Error is {ex}")
        return None, None, None, None

def determine_lat_long(place):
    try:
        geolocator = Nominatim(user_agent="https://maps.googleapis.com")
        location = geolocator.geocode(place)
        return location.latitude, location.longitude
    except Exception as ex:
        print(f"Exception occurred while trying to fetch lat and long: {place}. Error is {ex}")
        return None, None