"""download NAV and CTD files from same site"""
import re
from time import sleep
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
import pandas as pd
from ._utils import parse_file_path, make_names, create_error_message
from .utils import name_to_timestamp_dc


def _download_ts_helper(df, onc, category_code, params, output, f, fo, nworkers):
    """
    Helper function to download time series data
    """
    # start for loop
    nloop = 0
    futures = []
    log = open(output / "log.txt", "w", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=nworkers) as executor:

        for name, group in df.groupby(['deviceCode', 'gap']):

            date_from = group['timestamp'].iloc[0] - pd.Timedelta(10, "min")
            date_from = date_from.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            date_to = group['timestamp'].iloc[-1] + pd.Timedelta(25, "min")
            date_to = date_to.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

            date_o_from = str(group['timestamp'].iloc[0])[:-6]
            date_o_to = str(group['timestamp'].iloc[-1])[:-6]

            filters = {
                        'deviceCode': name[0],
                        'dateFrom'  : date_from,
                        'dateTo'    : date_to
                    }
            result = onc.getLocations(filters)


            # sanity check. Only one locationCode should be retrieved
            if len(result) > 1:
                raise RuntimeWarning("More than one location found for deviceCode",
                    name[0], " between ", date_from, " and ", date_to)
            location_code = result[0]['locationCode']
            location_code = location_code.split('.')[0]

            # get sublocations if they exist
            result2 = onc.getLocationHierarchy({
                'locationCode': location_code,
                'dateFrom'  : date_from,
                'dateTo'    : date_to
                })
            loc_children = result2[0]['children']
            location_code = [location_code]
            if loc_children is not None:
                location_code += [x['locationCode'] for x in loc_children]

            nloop += len(location_code) * len(category_code)
            for lc in location_code:
                for cc in category_code:
                    filters = {
                        'locationCode': lc,
                        'deviceCategoryCode': cc,
                        'dateFrom'  : date_from,
                        'dateTo'    : date_to,
                        "dataProductCode": "TSSD",
                        "extension" : "csv",
                    }

                    filters = filters | params

                    if fo is not None:
                        r = fo.loc[(fo['deviceCode']==name[0]) & (fo['dateFrom']==date_o_from) &
                            (fo['dateTo']==date_o_to) & (fo['locationCode']==lc) &
                            (fo['deviceCategoryCode']==cc)]
                        if r.shape[0] > 0:
                            continue

                    if cc == 'NAV':
                        filters['dpo_includeOrientationSensors'] = 'True'

                    to_write = f"{name[0]},{date_o_from},{date_o_to},{lc},{cc}"

                    futures.append(executor.submit(
                        _execute_download, onc, filters, output, f, to_write, log
                        ))

        pbar = tqdm(total=nloop, desc = 'Processed files')
        for _ in as_completed(futures):
            pbar.update()
        pbar.close()
    log.close()


def _execute_download(onc, filters, output, f, to_write, log):
    """
    function passed to threadE
    """
    params_request = {
        "method": "request",
        "token": onc.token
    }
    params_request = params_request | filters

    r1 = requests.get(
                "https://data.oceannetworks.ca/api/dataProductDelivery",
                params_request,
                timeout=10,
            )

    if r1.ok:
        json_result = r1.json()
        request_id = json_result["dpRequestId"]
    else:
        error_msg = create_error_message(r1)
        log.write(error_msg)
        return None

    processing = True
    while processing:
        r2 = requests.get(
            "https://data.oceannetworks.ca/api/dataProductDelivery",
            {
                "method": "run",
                "token": onc.token,
                "dpRequestId": request_id
            },
            timeout=10,
        )

        if r2.status_code == 202:
            sleep(2)
        elif r2.status_code == 200:
            processing = False
        else:
            error_msg = create_error_message(r2)
            log.write(error_msg)
            return None

    data = r2.json()
    for run in data:
        run_id = run["dpRunId"]
        file_count = run["fileCount"]

        for i in range(file_count):
            index = i + 1

            processing = True
            while processing:
                r3 = requests.get(
                    "https://data.oceannetworks.ca/api/dataProductDelivery",
                    {
                        "method": "download",
                        "token": onc.token,
                        "dpRunId": run_id,
                        "index": index
                    },
                    timeout=10, stream=True)

                if r3.status_code == 202:
                    sleep(2)
                elif r3.status_code == 200:
                    processing = False
                else:
                    error_msg = create_error_message(r3)
                    log.write(error_msg)
                    return None

            txt = r3.headers["Content-Disposition"]
            filename = txt.split("filename=")[1]
            with open(output / filename, 'wb') as file:
                for data in r3.iter_content(chunk_size=1024*1024):
                    file.write(data)
            f.write(f"{to_write},{filename}\n")


def download_ts(onc, source, category_code, output='output',
    options='fixed', nworkers=16):
    """
    Donwload timeseries data for video files

    Based on the filenames that are passed in the source, this function will
    download time series scalar data (tssd) that corresponds to the same
    time period as the filenames.

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *). If a DataFrame or a .csv file,
        it must have a column 'filename' that follow the ONC convention
        or columns 'timestamp' and 'deviceCode'.
    category_code : str or list
        Category Code of data to download. E.g. NAV, CTD, OXYSENSOR, etc.
    output : str, default 'output'
        Name of the output folder to save files.
    options : str, default 'fixed'
        Set options for search querry. If 'fixed', return clean resampled data
        for every minute, and maximum gap of one day between queries.
        If 'rov', return raw not resampled data, and set a maximum gap of one hour between queries.
    nworkers : int, default 16
        Number of simultaneous API calls to make
    """
    df, _, _ = parse_file_path(source, need_filename=False)

    if 'timestamp' in df and 'deviceCode' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df[['deviceCode', 'timestamp']]

    elif 'filename' in df:
        df = name_to_timestamp_dc(df['filename'])

    else:
        raise ValueError("Columns 'filename' or ('timestamp' and 'deviceCode') must be provided.")

    # fix a few parameters
    if options == 'rov':
        params = {
            'dpo_qualityControl': 0,
            'dpo_resample': 'none',
            'dpo_dataGaps': 0
        }
        gap = pd.Timedelta('1h')
    elif options == 'fixed':
        params = {
            'dpo_qualityControl': 1,
            'dpo_resample': 'average',
            'dpo_average': 60,
            'dpo_dataGaps': 0
        }
        gap = pd.Timedelta('1d')
    else:
        raise ValueError("options must be 'fixed' or 'rov'.")


    if not isinstance(category_code, list):
        category_code = [category_code]

    # check if command has been started alread
    output_pathlib = Path(output)
    file_out = output_pathlib / (output_pathlib.name + '.csv')
    if file_out.exists():
        fo = pd.read_csv(file_out)
        f = open(file_out, "a", encoding="utf-8")
    else:
        fo = None
        file_out.parent.mkdir(exist_ok=True)
        f = open(file_out, "w", encoding="utf-8")
        f.write("deviceCode,dateFrom,dateTo,locationCode,deviceCategoryCode,downloaded\n")

    # group if gap between timestamps is bigger than one day
    df.sort_values(['deviceCode', 'timestamp'], inplace=True)
    df['gap'] = (df.groupby('deviceCode')['timestamp'].diff() > gap).cumsum()

    # download files
    _download_ts_helper(df, onc, category_code, params, output_pathlib, f, fo, nworkers)

    f.close()


def _merge_ts_helper(data, tmp, tolerance):
    """
    Helper function to merge data based on timestamps
    """
    data.set_index('Time_UTC', inplace=True)
    data.interpolate(method='time', axis=0, inplace=True, limit_area='inside')
    tmp = pd.merge_asof(tmp, data, left_on='timestamp', right_index=True,
        suffixes=('', '_NEW'), tolerance=pd.Timedelta(tolerance, 's'), direction='nearest')

    cnames = tmp.columns.to_list()
    cnames_new = [cname for cname in cnames if '_NEW' in cname]

    for cname_new in cnames_new:
        cname = cname_new[:-4]
        tmp[cname] = tmp[cname].combine_first(tmp[cname_new])

    tmp.drop(columns=cnames_new, inplace=True)

    return tmp


def read_ts(file, units=True):
    """
    Read time series files from ONC and convert to a dataFrame

    Parameters
    ----------
    file : str
        Path to a .csv file of time series scalar data (tssd) returned from Oceans.
    units : bool, default True
        Include units of the vairables in the column names. If False, units are removed.

    Returns
    -------
    pandas.DataFrame
        A dataFrame from the csv file.
    
    """
    # Read the first 100 lines
    with open(file, 'r', encoding="utf-8") as f:
        r = [next(f) for _ in range(100)]

    # Find the index of '## END HEADER'
    n = next(i for i, line in enumerate(r) if '## END HEADER' in line) + 1

    # Extract column names
    cnames = r[n - 2][:-1]
    cnames = cnames.split(', ')
    cnames = [cname[1:-1] for cname in cnames]
    cnames[0] = 'Time_UTC'
    index = [not 'QC Flag' in cname for cname in cnames]

    if not units:
        cnames = [re.sub(r'\([^)]*\)', '', cname).rstrip() for cname in cnames]

    cnames = make_names(cnames)

    # Read the rest of the file using pandas
    out = pd.read_csv(file, skiprows=n, skipinitialspace=True, names=cnames)
    out = out.loc[:, index]
    out['Time_UTC'] = pd.to_datetime(out['Time_UTC'], format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)

    return out


def merge_ts(source, ts_data, tolerance=15, units=True):
    """
    Merge timeseries data with timestamps

    This function will get the timestamps from source and retrive the
    closest data avaiable inside the ts_data folder. If source is a
    DataFrame or a .csv file, it should have a column timestamp or
    filename, from which timestamps will be derived (filenames must follow
    Oceans naming convention)

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *). If a DataFrame or a .csv file,
        must have a column 'filename' that follow the ONC convention
        or columns 'timestamp' and 'deviceCode'.
    ts_data : str
        Folder containg csv files downloaded from Oceans 3
    tolerance : float
        Tolarance, in seconds, for timestamps to be merged. If the nearest
        data avaiable from a given timestamp is higher than the tolarance,
        then a NaN is returned instead.
    units : bool, default True
        Include units of the vairables in the column names. If False, units are removed.

    Returns
    -------
    pandas.DataFrame
        The dataFrame from source, with variables within ts_data
        merged based on the timestamps
    """
    df, _, _ = parse_file_path(source, need_filename=False)
    df.drop(columns='urlfile', inplace=True)

    cleanup = None
    if 'timestamp' in df:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        if 'deviceCode' not in df:
            print("only 'timestamp' column was provided. Assuming timestamps and data"
                "in 'ts_data' are not from multiple devices at the same time")
            df['deviceCode'] = "tmp"
            cleanup = ['deviceCode']

    elif 'filename' in df:
        df = pd.concat([df, name_to_timestamp_dc(df['filename'])], axis=1)
        cleanup = ['timestamp', 'deviceCode']

    else:
        raise ValueError("Columns 'filename' or ('timestamp' and 'deviceCode') must be provided")

    ts_folder = Path(ts_data)
    ts_folder_csv = ts_folder / (ts_data + '.csv')

    if ts_folder_csv.exists():
        ts_data = pd.read_csv(ts_folder_csv)
        devide_codes = df['deviceCode'].unique()
    else:
        print(f"File {ts_folder_csv.name} not found. Assuming all files in {ts_data}"
            "are from the same location. Make sure you are not merging data of different"
            "locations that overlap in time!")
        d = ts_folder.glob("*.csv")
        d = list(d)
        d = [x.name for x in d]
        devide_codes = [df['deviceCode'].iloc[0]]
        ts_data = pd.DataFrame({'deviceCode': df['deviceCode'].iloc[0], 'downloaded': d})
        df['deviceCode'] = df['deviceCode'].iloc[0]

    df.sort_values(['deviceCode', 'timestamp'], inplace=True)

    df_out = []
    for dc in devide_codes:
        ts_data_dc = ts_data[ts_data['deviceCode'] == dc]
        tmp = df[df['deviceCode'] == dc]

        for _, row in ts_data_dc.iterrows():
            data = read_ts(ts_folder / row['downloaded'], units)
            tmp = _merge_ts_helper(data, tmp, tolerance)

        df_out.append(tmp)

    df_out = pd.concat(df_out)

    if cleanup is not None:
        df_out.drop(columns=cleanup, inplace=True)

    return df_out
