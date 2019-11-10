#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import pathlib
import collections
from datetime import timedelta
from xmlrpc.client import ServerProxy

import geocoder
import toml
import prefect
from prefect.client import Secret
from prefect.schedules import IntervalSchedule


### TMP TMP TMP


import math
import collections

from dataclasses import dataclass

# Semi-axes of WGS-84 geoidal reference
WGS84_a = 6378137.0  # Major semiaxis [m]
WGS84_b = 6356752.3  # Minor semiaxis [m]


@dataclass
class Position:
    lat: float
    long: float

    def validate(self):
        if self.lat < -90 or self.lat > 90:
            raise ValueError(f"Invalid latitude {self.lat}! Must be in [-90, 90]")

        if self.long < -180 or self.long > 180:
            raise ValueError(f"Invalid longitude {self.long}! Must be in [-180, 180]")

@dataclass
class Area:
    point1: Position
    point2: Position
    
    def validate(self):
        for point in self.points:
            point.validate()

    @property
    def points(self):
        return (self.point1, self.point2)

    @property
    def lats(self):
        return (point.lat for point in self.points)

    @property
    def longs(self):
        return (point.long for point in self.points)

    @property
    def bbox(self):
        return (min(self.lats), max(self.lats), min(self.longs), max(self.longs))

# degrees to radians
def deg2rad(degrees):
    return math.pi*degrees/180.0

# radians to degrees
def rad2deg(radians):
    return 180.0*radians/math.pi

# Earth radius at a given latitude, according to the WGS-84 ellipsoid [m]
def wgs84_earth_radius(lat):
    # http://en.wikipedia.org/wiki/Earth_radius
    An = WGS84_a*WGS84_a * math.cos(lat)
    Bn = WGS84_b*WGS84_b * math.sin(lat)
    Ad = WGS84_a * math.cos(lat)
    Bd = WGS84_b * math.sin(lat)
    return math.sqrt( (An*An + Bn*Bn)/(Ad*Ad + Bd*Bd) )

# Bounding box surrounding the point at given coordinates,
# assuming local approximation of Earth surface as a sphere
# of radius given by WGS84
def surrounding_area(position: Position, halfSideInKm):
    lat = deg2rad(position.lat)
    lon = deg2rad(position.long)
    halfSide = 1000*halfSideInKm

    # Radius of Earth at given latitude
    radius = wgs84_earth_radius(lat)
    # Radius of the parallel at given latitude
    pradius = radius*math.cos(lat)

    latMin = lat - halfSide/radius
    latMax = lat + halfSide/radius
    lonMin = lon - halfSide/pradius
    lonMax = lon + halfSide/pradius

    return Area(Position(rad2deg(latMin), rad2deg(lonMin)), Position(rad2deg(latMax), rad2deg(lonMax)))


### TMP TMP TMP


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import calendar
import logging
import pprint
import requests
import enum

from datetime import datetime
from collections import defaultdict, namedtuple
import time

# import position
# import opensky

logger = logging.getLogger('opensky_api')
logger.addHandler(logging.NullHandler())

state_fields = ["icao24", "callsign", "origin_country", "time_position",
            "last_contact", "longitude", "latitude", "baro_altitude", "on_ground",
            "velocity", "heading", "vertical_rate", "sensors",
            "geo_altitude", "squawk", "spi", "position_source"]

AircraftState = namedtuple("AircraftState", " ".join(state_fields))

class PositionSource(enum.Enum):
    ADSB = 0
    ASTERIX = 1
    MLAT = 2
    FLARM = 3

class OpenSkyApi(object):

    def __init__(self, username=None, password=None):
        if username is not None:
            self._auth = (username, password)
        else:
            self._auth = ()
        self._api_url = "https://opensky-network.org/api"
        self._last_requests = defaultdict(lambda: 0)

    def _get_json(self, url_post, callee, params=None):
        r = requests.get("{0:s}{1:s}".format(self._api_url, url_post),
                         auth=self._auth, params=params, timeout=15.00)
        if r.status_code == 200:
            self._last_requests[callee] = time.time()
            return r.json()
        raise RuntimeError("Response not OK. Status {0:d} - {1:s}".format(r.status_code, r.reason))

    def _check_rate_limit(self, time_diff_noauth, time_diff_auth, func):
        if len(self._auth) < 2:
            return abs(time.time() - self._last_requests[func]) >= time_diff_noauth
        else:
            return abs(time.time() - self._last_requests[func]) >= time_diff_auth

    def get_states(self, time_secs=0, icao24=None, serials=None, bbox=None):
        """ Retrieve state vectors for a given time. If time = 0 the most recent ones are taken.
        Optional filters may be applied for ICAO24 addresses.
        :param time_secs: time as Unix time stamp (seconds since epoch) or datetime. The datetime must be in UTC!
        :param icao24: optionally retrieve only state vectors for the given ICAO24 address(es). The parameter can either be a single address as str or an array of str containing multiple addresses
        :param bbox: optionally retrieve state vectors within a bounding box. The bbox must be a tuple of exactly four values [min_latitude, max_latitude, min_longitude, max_latitude] each in WGS84 decimal degrees.
        :return: [StateVectors] if request was successful, None otherwise
        """
        logger = prefect.context.get("logger")
        if not self._check_rate_limit(10, 5, self.get_states):
            logger.debug("Blocking request due to rate limit")
            return None

        t = time_secs
        if type(time_secs) == datetime:
            t = calendar.timegm(t.timetuple())

        params = {"time": int(t), "icao24": icao24}

        if bbox != None:
            if isinstance(bbox, position.Area):
                bbox.validate()
                bbox_fields = bbox.bbox

                params["lamin"] = bbox_fields[0]
                params["lamax"] = bbox_fields[1]
                params["lomin"] = bbox_fields[2]
                params["lomax"] = bbox_fields[3]
            else:
                raise ValueError("Invalid bounding box!")

        states_json = self._get_json("/states/all", self.get_states, params=params)
        logger.info("States: " + repr(states_json))
        
        if states_json['states'] is None:
            return []
        return [AircraftState(*a) for a in states_json['states']]



### TMP TMP TMP

# to prevent the need to change imports/package roots

class position:
    Position = Position
    surrounding_area = surrounding_area
    Area = Area

### MAIN

class opensky:
    OpenSkyApi = OpenSkyApi
    PositionSource = PositionSource


@prefect.task
def fetch_current_area():
    # this is based on your current IP address
    current_position = position.Position(*geocoder.ip('me').latlng)
    current_position.validate()

    # bounding box within x KM radius of here
    area = position.surrounding_area(current_position, 20)
    area.validate()

    return area

@prefect.task
def fetch_above_aircraft(area: position.Area):
    logger = prefect.context.get("logger")
    username = os.environ.get('OPENSKY_USERNAME') or Secret("opensky_username").get()
    password = os.environ.get('OPENSKY_PASSWORD') or Secret("opensky_password").get()

    if username == None or len(username) == 0:
        raise ValueError("no username provided")

    if password == None or len(password) == 0:
        raise ValueError("no password provided")

    client = opensky.OpenSkyApi(username=username, password=password)
    result = client.get_states(bbox=area)

    if result is None:
        raise RuntimeError("no OpenSky API data returned")

    return result

# todo: publish this to a more interesting display...
@prefect.task
def update_display(ac_vectors):
    logger = prefect.context.get("logger")

    for aircraft in ac_vectors:
        if aircraft.callsign != None:
            logger.info("%-10s (from: %s)" % (aircraft.callsign, opensky.PositionSource(aircraft.position_source)))

    logger.info(f"Num AC: {len(ac_vectors)}")

    # log to inky screen service
    with ServerProxy("http://swag-pi-1.lan:5000/", allow_none=True) as proxy:
        proxy.register_buffer("whatsup", "center", 26)
        proxy.clear_buffer("whatsup")

    # for aircraft in ac_vectors:
    #     if aircraft.callsign != None:
    #         proxy.update_row("whatsup", aircraft.callsign, aircraft.callsign)

    # last row
    proxy.update_row("whatsup", "ZZZZZZZ", f"Aircraft: {len(ac_vectors)}")

    logger.info(f"Display complete!")


def all_py_files() -> dict:
    # this is bad: we need to interrogate a failed docker build to determine where to place files
    # return {str(path.absolute()): f"/root/.prefect/{str(path)}" for path in pathlib.Path('.').glob('**/*.py')}
    return {str(path.absolute()): str(path) for path in pathlib.Path('.').glob('**/*.py')}

# does not work all the time! (mixed dependencies)
def python_pip_requirements_from_lock() -> list:
    packages = []
    dependencies = toml.load("poetry.lock")
    for package in dependencies['package']:
        packages.append(f"{package['name']}=={package['version']}")
    return packages

def python_pip_requirements_from_project(loose=True) -> list:
    packages = []
    settings = toml.load("pyproject.toml")
    for name, version in settings['tool']['poetry']['dependencies'].items():
        if name == 'python':
            continue
        if loose:
            packages.append(name)
        else:
            packages.append(f"{name}=={version}")
    return packages

def python_pip_requirements() -> list:
    with open('requirements.txt', 'r') as f:
        return [req.strip() for req in f.readlines()]

def main():
    prefect.context.setdefault("secrets", {})
    secrets = prefect.context.secrets
    secrets['opensky_username'], secrets['opensky_password'] = os.environ['OPENSKY_USERNAME'], os.environ['OPENSKY_PASSWORD']

    schedule = IntervalSchedule(interval=timedelta(minutes=1))

    with prefect.Flow("whats-up", schedule) as flow:
        area = fetch_current_area()
        aircraft = fetch_above_aircraft(area=area)
        update_display(ac_vectors=aircraft)

        # run locally
        # flow.run()

        # deploy to cloud (under the "alex" project)
        flow.deploy("test", 
                    # optional when locally run with agent
                    base_image="quay.io/wagoodman/prefect-arm:0.7.0-python3.7",
                    # prefect_version="0.7.0",
                    registry_url="quay.io/wagoodman",
                    python_dependencies=python_pip_requirements(),
                    # files=all_py_files(),
                    local_image=True,
                    )

if __name__ == '__main__':
    main()
