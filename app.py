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
from prefect import config
from prefect.client import Secret
from prefect.schedules import IntervalSchedule
from prefect.runtimes import DevCLI

import opensky
import position

@prefect.task()
def fetch_current_area():
    # this is based on your current IP address
    current_position = position.Position(*geocoder.ip('me').latlng)
    current_position.validate()

    # bounding box within x KM radius of here
    area = position.surrounding_area(current_position, 35)
    area.validate()

    return area

@prefect.task()
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


@prefect.task()
def update_display(ac_vectors):
    logger = prefect.context.get("logger")

    for aircraft in ac_vectors:
        if aircraft.callsign != None:
            logger.info("%-10s (from: %s)" % (aircraft.callsign, opensky.PositionSource(aircraft.position_source)))

    logger.info(f"Num AC: {len(ac_vectors)}")

    # log to inky screen service
    try:
        with ServerProxy("http://swag-pi-1.lan:5000/", allow_none=True) as proxy:
            proxy.register_buffer("title", "upperleft", 18)
            proxy.register_buffer("whatsup", "centerleft", 45)
            proxy.register_buffer("aircraft", "upperright", 12)
            proxy.clear_buffer("whatsup")
            proxy.clear_buffer("aircraft")

        proxy.update_row("title", "0", "Aircraft:")

        ac_with_callsigns = [ac for ac in ac_vectors if ac.callsign != None and len(str(ac.callsign).strip()) > 0]

        proxy.update_row("whatsup", "0", f" {len(ac_with_callsigns)}")

        rows = []
        cur_row = []
        for ac in ac_with_callsigns:
            cur_row.append(ac.callsign)
            if len(cur_row) == 3:
                rows.append(' '.join(cur_row))
                cur_row = []
        
        if len(cur_row) > 0:
            rows.append(' '.join(cur_row))
            cur_row = []
                
        for idx, row in enumerate(rows):
            proxy.update_row("aircraft", str(idx), row)

        logger.info(f"Display complete!")
    except:
        logger.error(f"Unable to display!")


def main():

    with prefect.Flow("whats-up") as flow:
        area = fetch_current_area()
        aircraft = fetch_above_aircraft(area=area)
        update_display(ac_vectors=aircraft)

        runtime = DevCLI(flow=flow)
        runtime.run()


if __name__ == '__main__':
    main()
