"""Utility functions also exported by the user"""
import os
import pandas as pd
from onc.onc import ONC

def onc(token = None):
    """
    Create an ONC class object
    
    Create an ONC class object, but try to use environment variables to get the API token.
    The token must stored under 'ONC_API_TOKEN'.

    Parameters
    ----------
    token : str
        ONC API token

    Returns
    -------
    onc.ONC
        ONC class object
    """
    token = token if token else os.getenv('ONC_API_TOKEN')
    if token is None:
        raise ValueError("No API credentials were provided!")
    return ONC(token)


def name_to_timestamp(filename):
    """
    Get timestamp from a filename
    
    Parameters
    ----------
    filename : str
        A filename that follows the ONC convention (deviceCode_timestampUTC.ext).

    Returns
    -------
    pandas.Timestamp
        The timestamp object corresponding to the filename. The object also has attributes
        'dc' (deviceCode) and 'ext' (file extension) for convenience.
    """
    r0_split = filename.split('_')
    r1_split = r0_split[-1].split('.')

    if '-' in r1_split[1]:
        r1_split[1], bitrate = r1_split[1].split('-')
        ext = f"-{bitrate}.{r1_split[2]}"
    else:
        ext = f".{r1_split[2]}"

    r1 = f'{r1_split[0]}.{r1_split[1]}'
    r1 = pd.to_datetime(r1, format='%Y%m%dT%H%M%S.%fZ', utc=True)
    r1.dc = '_'.join(r0_split[:-1])
    r1.ext = ext

    return r1


def name_to_timestamp_dc(filenames):
    """
    Get timestamps and deviceCode from filenames

    Wrapper for name_to_timestamp to return deviceCode and
    timestamp from a pandas Series.
    
    Parameters
    ----------
    filenames : pandas.Series
        A str series with filenames that follow the ONC convention (deviceCode_timestampUTC.ext).

    Returns
    -------
    pandas.DataFrame
        A DataFrame with timestamps and deviceCodes
    """
    def _name_to_timestamp_dc_helper(filename):
        x = name_to_timestamp(filename)
        return x, x.dc

    tmp = filenames.apply(_name_to_timestamp_dc_helper).to_list()
    return pd.DataFrame(tmp, columns=['timestamp', 'deviceCode'], index=filenames.index)
