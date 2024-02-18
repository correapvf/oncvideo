"""Wrapper to get API key from system enviroment"""
import os
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
