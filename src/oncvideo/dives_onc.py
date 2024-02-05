"""Get a table with all ROV dives from SeaTube V3"""
import json
import requests
import pandas as pd

def get_dives(onc, add_location=True):
    """
    Return all dives from Oceans3.0

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    add_location : bool, default True
        Get locationCodes from the dives as well.

    Returns
    -------
    pandas.DataFrame
        A DataFrame that includes dives code, start and end times
        for the dive, deviceCode and deviceId.
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
    cols = ['dive','organization','year','expedition','startDate','endDate','id','deviceId',
            'deviceCode','deviceName']

    if add_location:
        data = onc.getDeployments(filters)
        locations = pd.DataFrame(data)
        locations = locations[['deviceCode', 'locationCode']]
        locations.drop_duplicates(inplace=True)

        df = pd.merge(df, locations, how="left", on="deviceCode")
        cols = cols + ['locationCode','location']

    return df[cols]
