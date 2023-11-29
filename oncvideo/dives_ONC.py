import json
import requests
import pandas as pd

def getDives(onc, add_location=True):
    # get table with all dives
    URL = 'https://data.oceannetworks.ca/expedition/tree'
    data = json.loads(requests.get(URL).text)

    df = pd.json_normalize(data['payload']['videoTreeConfig'][0],
        ['children', 'children', 'children', 'children'],
        [['children', 'html'],
        ['children', 'children', 'html'],
        ['children', 'children', 'children', 'html']])

    df = df[df["ready"] == True]
    new = df["html"].str.split(" - ", n = 2, expand = True)

    df.rename(columns={"children.html": "organization",
                    "children.children.html": "year",
                    "children.children.children.html": "expedition"},
                    inplace = True)
    # df = df[['organization', 'year', 'expedition', 'defaultDeviceId', 'id', 'startDate', 'endDate']]
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
