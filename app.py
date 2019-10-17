#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import collections

import geocoder
import prefect

import opensky
import position


@prefect.task
def fetch_current_area():
    # this is based on your current IP address
    current_position = position.Position(*geocoder.ip('me').latlng)
    current_position.validate()

    # bounding box within x KM radius of here
    area = position.surrounding_area(current_position, 10)
    area.validate()

    return area

@prefect.task
def fetch_above_aircraft(area: position.Area):
    logger = prefect.context.get("logger")
    secrets = prefect.context.secrets

    client = opensky.OpenSkyApi(password=secrets['opensky_password'], username=secrets['opensky_username'])
    result = client.get_states(bbox=area)

    if result is None:
        raise RuntimeError("no OpenSky API data returned")

    return result

# todo: publish this to a more interesting display...
@prefect.task
def update_display(ac_vectors):
    logger = prefect.context.get("logger")

    ### tmp display code...
    for aircraft in ac_vectors:
        if aircraft.callsign != None:
            logger.info("%-10s (from: %s)" % (aircraft.callsign, opensky.PositionSource(aircraft.position_source)))

    logger.info(f"Num AC: {len(ac_vectors)}")
    ### tmp display code

def main():
    prefect.context.setdefault("secrets", {})
    secrets = prefect.context.secrets
    secrets['opensky_username'], secrets['opensky_password'] = os.environ['OPENSKY_USERNAME'], os.environ['OPENSKY_PASSWORD']

    with prefect.Flow("aircraft positions") as flow:
        area = fetch_current_area()
        aircraft = fetch_above_aircraft(area=area)
        update_display(ac_vectors=aircraft)

        # run locally
        flow.run()

if __name__ == '__main__':
    main()