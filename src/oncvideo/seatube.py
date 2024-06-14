"""easy download files from Seatube V3"""
from urllib.parse import urlparse, parse_qs
import json
import requests
import pandas as pd
from tqdm import tqdm
from .utils import name_to_timestamp, name_to_timestamp_dc
from ._utils import parse_file_path, URL, strftd2
from .dives_onc import get_dives

def download_st(onc, url, ext='mov'):
    """
    Generate download link from Seatube

    Generate download link for the correspoding video based on the Seatube link provided.
    Useful for downloading the high-res video that is not avaiable in Seatube.
    Only supports Seatube V3.

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    url : str
        The link generated by Seatube V3. E.g.
        `https://data.oceannetworks.ca/SeaTubeV3?resourceTypeId=600&resourceId=4371&time=2023-09-13T20:43:09.000Z`
        or csv file with 'seatube_link' column.
    ext : str, default mov
        Especify a extension of the video to be downloaded.
    """
    if url.endswith('.csv'):
        df = pd.read_csv(url)
        urls = df['seatube_link']
        df['url'] = ''
        df['video_time'] = ''
        df['frame_filename'] = ''

        for index, value in tqdm(urls.items(), tota=urls.shape[0]):
            urlfile, ss, new_name = _download_st_helper(value, ext, onc)
            df.loc[index, 'url'] = urlfile
            df.loc[index, 'video_time'] = ss
            df.loc[index, 'frame_filename'] = new_name

        df.to_csv("videos_seatube.csv", index=False)

    else:
        urlfile, ss, new_name = _download_st_helper(url, ext, onc)
        print(urlfile)
        print("Frame expected at ", ss)
        print("File name for the frame: ", new_name)


def _download_st_helper(url, ext, onc):
    """
    Helper function to get archived file from link and download it
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

    # get correct time and new filename
    ss = strftd2(tmp['ts'] - tmp['file_ts'], div=':')
    timestamp_str = timestamp.strftime('%Y%m%dT%H%M%S.%f')[:-3]+'Z'
    new_name = f"{filters['deviceCode']}_{timestamp_str}.jpg"

    return urlfile, ss, new_name

    


def _generate_link(timediff, idd, ts):
    """
    Generate link to Seatube V3
    """
    if timediff:
        ts = ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        url = 'https://data.oceannetworks.ca/SeaTubeV3'
        return f"{url}?resourceTypeId=600&resourceId={idd}&time={ts}Z"
    else:
        return ''


def link_st(onc, source, dive=False):
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
        it must have a column 'filename' that follow the ONC convention
        or columns 'timestamp' and 'deviceCode'.
    dive : bool, default False
        Include a column in the output with dives where the video is from

    Returns
    -------
    pandas.DataFrame
        The dataFrame from source, with new column `url` with corresponding
        Seatube links.
    """
    df, _, _ = parse_file_path(source, need_filename=False)
    df.drop(columns='urlfile', inplace=True)

    cols = ['id','startDate','endDate']
    if dive:
        cols += ['dive']

    index = df['filename'].str.count('\\.') == 1
    df.loc[index, 'filename'] = df['filename'].str.replace('.', '.000Z.', regex=False)

    if 'timestamp' in df and 'deviceCode' in df:
        cleanup = False
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    elif 'timestamp' in df or 'deviceCode' in df:
        raise ValueError("Both columns 'timestamp' and 'deviceCode' must be provided.")

    elif 'filename' in df:
        cleanup = True
        df = pd.concat([df, name_to_timestamp_dc(df['filename'])], axis=1)

    else:
        raise ValueError("Columns 'filename' or ('timestamp' and 'deviceCode') must be provided.")

    df.sort_values('timestamp', inplace=True)

    dives = get_dives(onc)
    dives = dives[dives['deviceCode'].isin(df['deviceCode'].unique())]
    dives = dives[cols]

    if dives.shape[0] > 0:
        dives['startDate'] = pd.to_datetime(dives['startDate'], utc=True)
        dives['endDate'] = pd.to_datetime(dives['endDate'], utc=True)
        dives.sort_values('startDate', inplace=True)

        df = pd.merge_asof(df, dives, left_on='timestamp', right_on='startDate')
        df['timediff'] = (df['endDate'] - df['timestamp']).dt.total_seconds() > 0

        df['url'] = df.apply(lambda x: _generate_link(x.timediff, x.id, x.timestamp), axis=1)
        df.drop(columns=['id','startDate','endDate','timediff'], inplace=True)

    else:
        df['url'] = ''

    if cleanup:
        df.drop(columns=['timestamp','deviceCode'], inplace=True)

    return df


def rename_st(filename):
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