"""download NAV and CTD files from same site"""
import os
import sys
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
from ._utils import parse_file_path
from .utils import name_to_timestamp_dc


class _HiddenPrints:
    """
    To prevent onc to print output when downloading files
    """
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def _download_ts_helper(df, onc, category_code, clean, f, fo, nworkers):
    """
    Helper function to download time series data
    """
    # get timestamp and deviceCode from filename
    df = df[['filename']].copy()
    df = pd.concat([df, name_to_timestamp_dc(df['video_filename'])], axis=1, ignore_index=True)
    df.sort_values(['deviceCode', 'timestamp'], inplace=True)

    # group if gap between timestamps is bigger than one day
    df['gap'] = (df.groupby('deviceCode')['timestamp'].diff() > pd.Timedelta(1, "d")).cumsum()

    # start for loop
    nloop = 0
    futures = []

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

            location_code = location_code.split('.')
            if len(location_code) > 1:
                result2 = onc.getLocationHierarchy({'locationCode': location_code[0]})
                location_code = [x['locationCode'] for x in result2]

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
                        'dpo_qualityControl': clean,
                        'dpo_resample': 'none',
                        'dpo_dataGaps': 0
                    }

                    if fo is not None:
                        r = fo.loc[(fo['deviceCode']==name[0]) & (fo['dateFrom']==date_o_from) &
                            (fo['dateTo']==date_o_to) & (fo['locationCode']==lc) &
                            (fo['deviceCategoryCode']==cc)]
                        if r.shape[0] > 0:
                            continue

                    if cc == 'NAV':
                        filters['dpo_includeOrientationSensors'] = 'True'

                    to_write = f"{name[0]},{date_o_from},{date_o_to},{lc},{cc}"

                    futures.append(executor.submit(_execute_download, onc, filters, f, to_write))

        pbar = tqdm(total=nloop, desc = 'Processed files')
        for _ in as_completed(futures):
            pbar.update()
        pbar.close()


def _execute_download(onc, filters, f, to_write):
    """
    function passed to thread
    """
    try:
        with _HiddenPrints():
            result3 = onc.orderDataProduct(filters, includeMetadataFile=False)

        for file in result3['downloadResults']:
            # write in csv file
            f.write(f"{to_write},{file['file']}\n")
    except Exception:
        pass


def download_ts(onc, source, category_code, output='output',
    clean=True, out_merged=False, nworkers=20, **kwargs):
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
        match multiple files (use *).
    category_code : str or list
        Category Code of data to download. E.g. NAV, CTD, OXYSENSOR, etc.
    output : str, default 'output'
        Name of the output folder to save files.
    clean : bool, default True
        Return clean data from the API call (values with bad flags are removed),
        else return raw data.
    out_merged : bool, default False
        If True, the function :func:`~oncvideo.merge_ts` will run automatically
        after tssd is downloaded and save the merged file as `{output}_merged.csv`.
    nworkers : int, default 20
        Number of simultaneous API calls to make
    **kwargs
        `tolerance` and `units` passed to :func:`~oncvideo.merge_ts`

    Returns
    -------
    pandas.DataFrame
        The dataFrame generated from source, with variables within ts_data
        merged based on the timestamps
    """
    df, _, _ = parse_file_path(source)

    tol = kwargs.get('tolerance', 15)
    units = kwargs.get('units', True)

    # fix a few parameters
    clean = 0 if clean else 1

    if not isinstance(category_code, list):
        category_code = [category_code]

    out_path = onc.outPath
    onc.outPath = output

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

    # download files
    try:
        _download_ts_helper(df, onc, category_code, clean, f, fo, nworkers)

    finally:
        f.close()
        onc.outPath = out_path

    if out_merged:
        out_file = output + '_merged.csv'
        out = merge_ts(df, output, tol, units)
        out.to_csv(out_file, index=False, na_rep='NA')


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
    cnames = r[n - 2]
    cnames = cnames.split(', ')
    cnames = [cname[1:-1] for cname in cnames]
    cnames[0] = 'Time_UTC'
    index = [not 'QC Flag' in cname for cname in cnames]

    if not units:
        cnames = [re.sub(r'\([^)]*\)', '', cname).rstrip() for cname in cnames]

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
    df, _, _ = parse_file_path(source, need_filename = False)
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
        df = pd.concat([df, name_to_timestamp_dc(df['video_filename'])], axis=1, ignore_index=True)
        cleanup = ['timestamp', 'deviceCode']

    else:
        raise ValueError("Columns 'filename' or ('timestamp' and 'deviceCode') must be provided")

    ts_folder = Path(ts_data)
    ts_folder_csv = ts_folder / (ts_data + '.csv')

    if ts_folder_csv.exists():
        ts_data = pd.read_csv(ts_folder_csv)
    else:
        print(f"File {ts_folder_csv.name} not found. Assuming all files in {ts_data}"
            "are from the same location. If timelapses and files are from different"
            "locations and overlap in time, the merge operations will be wrong!")
        d = ts_folder.glob("*.jpg")
        ts_data = pd.DataFrame({'deviceCode': 'tmp', 'downloaded': list(d)})

    df.sort_values(['deviceCode', 'timestamp'], inplace=True)

    df_out = []
    for dc in df['deviceCode'].unique():
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
