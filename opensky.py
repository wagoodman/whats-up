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

import position

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
        
        return [AircraftState(*a) for a in states_json['states']]