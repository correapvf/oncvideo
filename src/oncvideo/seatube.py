"""easy download files from Seatube V3"""
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import json
import requests
import pandas as pd
from ._utils import name_to_timestamp, name_to_timestamp_dc, parse_file_path
from ._utils import run_ffmpeg, download_file, URL, strftd2
from .dives_onc import get_dives

def st_download(onc, url, ext='mov', ext_frame=False):
    """
    Download video from Seatube

    Download the correspoding video based on the Seatube link provided.
    Useful for downloading the high-res video that is not avaiable in Seatube.
    Only supports Seatube V3. Video is saved at current working directory.

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    url : str
        The link generated by Seatube V3. E.g.
        `https://data.oceannetworks.ca/SeaTubeV3?resourceTypeId=600&resourceId=4371&time=2023-09-13T20:43:09.000Z`
    ext : str, default mov
        Especify a extension of the video to be downloaded.
    ext_frame : bool, default False
        If True, also extract a frame correspoding to the same timestamp as the url.
    """
    parsed_url = urlparse(url)
    parsed_query = parse_qs(parsed_url.query)
    dive_id = parsed_query['resourceId'][0]

    timestamp = pd.to_datetime(parsed_query['time'][0],
        format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
    date_from = timestamp - pd.to_timedelta(60, unit='m')
    date_to = timestamp + pd.to_timedelta(60, unit='m')

    ts = pd.DataFrame({'ts': timestamp}, index=[0])

    url = 'https://data.oceannetworks.ca/seatube/details'
    params = {'diveId': dive_id}
    data = json.loads(requests.get(url, params=params, timeout=10).text)

    filters = {
            'deviceCode': data['payload']['deviceCode'],
            'dateFrom'  : date_from.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z',
            'dateTo'    : date_to.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z',
            'extension' : ext
        }

    result = onc.getListByDevice(filters, allPages=True)
    result = result['files']

    df = pd.DataFrame({'filename': result})
    df['file_ts'] = df['filename'].apply(name_to_timestamp)

    # check for Quality
    if df['filename'].str.contains('Z-', regex=False).any():

        df['quality'] = df['filename'].str.replace('Z.', 'Z-0.', regex=False)
        df['quality'] = df['quality'].str.split('-').str[-1].str.split('.').str[0]
        qualityn = df['quality'].value_counts()

        if len(qualityn) > 1:
            quality = df['quality'].max()
            df = df[df['quality'] == quality]

    # merge based on prior match
    tmp = pd.merge_asof(ts, df, left_on='ts', right_on='file_ts')
    tmp = tmp.loc[0]

    urlfile = URL + tmp['filename']
    download_file(urlfile, Path(tmp['filename']))

    if ext_frame:
        ss = tmp['ts'] - tmp['file_ts']
        ss_str = str(ss.total_seconds())

        timestamp_str = timestamp.strftime('%Y%m%dT%H%M%S.%f')[:-3]+'Z'
        new_name = f"{filters['deviceCode']}_{timestamp_str}.jpg"

        ff_cmd = ['ffmpeg', '-ss', ss_str, '-i', tmp['filename']] + ['-frames:v', '1',
            '-update', '1', '-qmin', '1', '-qmax', '1', '-q:v', '1', new_name]
        run_ffmpeg(ff_cmd, filename=tmp['filename'])

        print("Frame extracted at ", strftd2(ss))


def generate_link(timediff, idd, ts):
    """
    Generate link to Seatube V3
    """
    if timediff:
        ts = ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        url = 'https://data.oceannetworks.ca/SeaTubeV3'
        return f"{url}?resourceTypeId=600&resourceId={idd}&time={ts}Z"
    else:
        return ''


def st_link(onc, source):
    """
    Generate Seatube link

    Generate a Seatube link from filenames following the Oceans 3 naming
    convention (deviceCode_timestamp.ext). For now, only supports video avaiable
    in Seatube V3 (videos from ROV cameras).

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *). If a DataFrame or a .csv file,
        it must have a column 'filename' and, optionally, 'timestamp'.

    Returns
    -------
    pandas.DataFrame
        The dataFrame from source, with new column `url` with corresponding
        Seatube links.
    """
    if '*' in source or source.endswith('.csv'):
        df, _, _ = parse_file_path(source)
        df.drop(columns='urlfile', inplace=True)
    else:
        if not '.' in source:
            raise ValueError("Source must be a valid name with extension.")
        df = pd.DataFrame({'filename': [source]}, index=[0])
    ts_bk = None

    index = df['filename'].str.count('\\.') == 1
    df.loc[index, 'filename'] = df['filename'].str.replace('.', '.000Z.', regex=False)

    if 'filename' in df:
        df[['ts', 'dc']] = df['filename'].apply(name_to_timestamp_dc).to_list()

        if 'timestamp' in df:
            ts_bk = df['timestamp']
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df.drop(columns='ts', inplace=True)
        else:
            df.rename(columns={'ts': 'timestamp'}, inplace=True)

    df.sort_values(['dc', 'filename'], inplace=True)

    dives = get_dives(onc)
    dives = dives[['deviceCode','id','startDate','endDate']]
    dives = dives[dives['deviceCode'].isin(df['dc'].unique())]

    if dives.shape[0] > 0:
        dives['startDate'] = pd.to_datetime(dives['startDate'], utc=True)
        dives['endDate'] = pd.to_datetime(dives['endDate'], utc=True)
        dives.sort_values('startDate', inplace=True)

        df = pd.merge_asof(df, dives, left_on='timestamp', right_on='startDate')
        df['timediff'] = (df['endDate'] - df['timestamp']).dt.total_seconds() > 0

        df['url'] = df.apply(lambda x: generate_link(x.timediff, x.id, x.timestamp), axis=1)
        df.drop(columns=['deviceCode','id','startDate','endDate','timediff'], inplace=True)

    else:
        df['url'] = ''

    if ts_bk is None:
        df.drop(columns=['timestamp', 'dc'], inplace=True)
    else:
        df['timestamp'] = ts_bk
        df.drop(columns='dc', inplace=True)

    return df


def st_rename(filename):
    """
    Rename framegrabs from Seatube to correct timestamp

    Correct the timestamp based in the offest of the image that is
    captured in Seatube. E.g. a file like `deviceCode_20230913T200203.000Z-003.jpeg`
    is renamed to `deviceCode_20230913T200201.000Z.jpeg`.

    Parameters
    ----------
    filename : str
        Filename to be corrected.

    Returns
    -------
    str
        If possible, the corrected filename, or else it will return the same filename.
    """
    newname = filename
    timestamp = name_to_timestamp(filename)
    if timestamp.ext[0] == '-':
        offset = timestamp.ext[1:4]
        if offset.isdigit():
            offset = int(offset)
            if 1 <= offset <= 9:
                offset = offset - 5
                ts = timestamp + pd.to_timedelta(offset, unit='sec')
                ts = ts.strftime('%Y%m%dT%H%M%S.%f')[:-3]
                suffix = filename.split('.')[-1]
                newname = f"{timestamp.dc}_{ts}Z.{suffix}"
    return newname
