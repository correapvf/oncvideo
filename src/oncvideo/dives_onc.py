"""Get a table with all ROV dives from SeaTube V3"""
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

def _check_time(dives):
    """
    Check if file as modify less than 30 days ago
    """
    modify_timestamp = dives.stat().st_mtime
    modify_date = datetime.fromtimestamp(modify_timestamp)
    timediff = datetime.now() - modify_date
    return timediff < timedelta(days=30)


def _get_dives_api(onc):
    """
    function to get dives table from Seatube V3
    """
    # get table with all dives
    url = 'https://data.oceannetworks.ca/expedition/tree'
    data = json.loads(requests.get(url, timeout=10).text)

    df = pd.json_normalize(data['payload']['videoTreeConfig'][0],
        ['children', 'children', 'children', 'children'],
        [['children', 'html'],
        ['children', 'children', 'html'],
        ['children', 'children', 'children', 'html']])

    df["ready"].fillna(False, inplace=True)
    df = df[df["ready"]]
    new = df["html"].str.split(" - ", n = 2, expand = True)

    df.rename(columns={"children.html": "organization",
                    "children.children.html": "year",
                    "children.children.children.html": "expedition"},
                    inplace = True)

    df["dive"] = new[0]
    df["location"] = new[2]
    df.drop(columns='deviceId', inplace=True)
    df.rename(columns={"defaultDeviceId": "deviceId"}, inplace=True)

    # get table with deployments and devices
    filters = {'deviceCategoryCode': 'ROV_CAMERA'}

    data = onc.getDevices(filters)
    devices = pd.DataFrame(data)
    devices = devices[['deviceCode', 'deviceId', 'deviceName']]

    df = pd.merge(df, devices, how="left", on="deviceId")

    # get locations code
    data = onc.getDeployments(filters)
    locations = pd.DataFrame(data)
    locations = locations[['deviceCode', 'locationCode']]
    locations.drop_duplicates(inplace=True)

    df = pd.merge(df, locations, how="left", on="deviceCode")

    cols = ['dive','organization','year','expedition','startDate','endDate','id','deviceId',
        'deviceCode','deviceName','locationCode','location']

    return df[cols]


def get_dives(onc, cache=True):
    """
    Return all dives from Oceans3.0

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    cache : bool, default True
        Save file at home folder and use it for future calls if the
        file is recent (less the 30 days).

    Returns
    -------
    pandas.DataFrame
        A DataFrame that includes dives code, start and end times
        for the dive, deviceCode and locationCode.
    """
    dives = Path.home() / ".dives_ONC.csv"

    if cache and dives.is_file() and _check_time(dives):
        df = pd.read_csv(dives)

    else:
        # get a new dive file
        df = _get_dives_api(onc)
        if cache:
            df.to_csv(dives, index=False)

    return df
