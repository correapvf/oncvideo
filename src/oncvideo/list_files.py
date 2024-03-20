"""List archived files from ONC"""
import pandas as pd
from ._utils import sizeof_fmt, strftd
from .utils import name_to_timestamp
from .dives_onc import get_dives

def _ask_options(results, ivalue, ihelp):
    """
    List option and ask user to select which result they want to keep
    """
    noptions = len(results)
    input_message = f'Select a {ivalue}:\n'
    for index in range(noptions):
        item = results[index][ivalue]
        item2 = results[index][ihelp]
        input_message += f'{index}) {item} ({item2})\n'
    input_message += 'Enter a number: '

    while True:
        value = input(input_message)
        check = not value.isnumeric()
        if check:
            print('Value must be numeric.')
            continue
        value = int(value)
        check = value > noptions
        if check:
            print(f'Value must be equal or below {noptions}.')
        else:
            break

    out = results[value][ivalue]
    print('You selected: ' + out)
    return out

def _ask_options_multiple(results, ivalue):
    """
    List option and ask user to select which result they want to keep
    Allows multiple values to be choosen
    """
    results = pd.concat([results, pd.Series([results.sum()], index=['all'])])
    input_message = f"Select a {ivalue}:\n"
    for index, item in enumerate(results.items()):
        input_message += f'{index}) {item[0]} ({item[1]} videos)\n'
    input_message += 'Enter a number (use comma to separate multiple options): '

    noptions = results.shape[0]
    while True:
        value = input(input_message)
        value = value.split(',')
        check = not all([i.isnumeric() for i in value])
        if check:
            print('Values must be numeric.')
            continue
        value = [int(i) for i in value]
        check = not all([i <= noptions for i in value])
        if check:
            print(f'Values must be equal or below {noptions}.')
        else:
            break

    value = [int(i) for i in value]
    out = ','.join(results.index[value])
    print('You selected: ' + out)
    return out


def _list_file_helper(df, statistics, extension, quality, cols_to_keep):
    """
    Helper function to get the API output and filter unwanted
    files and format columns
    """
    cols = df.columns.to_list()
    if 'group' in cols:
        cols_new = ['group', 'filename']
        cols_sort = ['group', 'ext']
    else:
        cols_new = ['filename']
        cols_sort = ['ext']


    # select extension
    df['ext'] = df['filename'].str.split('.').str[-1]
    extn = df['ext'].value_counts()

    if len(extn) > 1:
        if extension == 'ask':
            extension = _ask_options_multiple(extn, "extension")

        if extension != 'all':
            extension = extension.split(',')
            df = df[df['ext'].isin(extension)].copy()

    # select quality
    if df['filename'].str.contains('Z-', regex=False).any():

        df['quality'] = df['filename'].str.replace('Z.', 'Z-standard.', regex=False)
        df['quality'] = df['quality'].str.split('-').str[-1].str.split('.').str[0]
        qualityn = df['quality'].value_counts()

        if len(qualityn) > 1:

            if quality == 'ask':
                quality = _ask_options_multiple(qualityn, "quality")

            if quality != 'all':
                quality = quality.split(',')
                df = df[df['quality'].isin(quality)]

            qualityn = df['quality'].value_counts()

        # Only keep quality column if more than one quality after filter
        if len(qualityn) > 1 or quality == 'all':
            cols_new += ['quality']
            cols_sort += ['quality']

    # Start and End columns
    df['ext'] = df['filename'].str.split('.').str[-1]

    df.sort_values(cols_sort + ['filename'], inplace=True, ignore_index=True)
    df['query_offset'] = ''
    timef = '%Y-%m-%dT%H:%M:%S.%fZ'

    for _, group in df.groupby(cols_sort):
        
        if group['dateFromQuery'].iloc[0] is not None:
            # first file
            r1 = name_to_timestamp(group['filename'].iloc[0])
            r01 = pd.to_datetime(group['dateFromQuery'].iloc[0], utc=True, format=timef)
            timediff = r01 - r1
            nseconds = timediff.total_seconds()
            if nseconds > 0:
                query = f'start at {strftd(abs(nseconds))}'
            elif nseconds < 0:
                query = f'gap dateFrom of {timediff * -1}'
            else:
                query = ''
            df.loc[group.index[0],'query_offset'] = query
        
        if group['dateToQuery'].iloc[-1] is not None:
            # last file
            r2 = name_to_timestamp(group['filename'].iloc[-1])
            r02 = pd.to_datetime(group['dateToQuery'].iloc[-1], utc=True, format=timef)
            timediff = r02 - r2
            nseconds = timediff.total_seconds()
            if 0 < nseconds < 1800: # 30 min * 60
                query = f'end at {strftd(abs(nseconds))}'
            elif nseconds <= 0:
                query = f'querry dateTo is before by {timediff * -1}'
            else:
                query = f'gap dateTo of {timediff}'
            query0 = df.loc[group.index[-1],'query_offset']
            if query0 == '':
                df.loc[group.index[-1],'query_offset'] = query
            else:
                df.loc[group.index[-1],'query_offset'] = f"{query0}/{query}"

    if cols_to_keep is not None:
        cols_new += cols_to_keep

    # calculate and print overall statistics
    print('Number of files: ', df.shape[0])
    if statistics:
        cols_new += ['duration','fileSizeMB','year','month','day','hour','minute','second']
        df['dateFrom'] = pd.to_datetime(df['dateFrom'], format=timef, utc=True)
        df['dateTo'] = pd.to_datetime(df['dateTo'], format=timef, utc=True)
        df['duration'] = df['dateTo'] - df['dateFrom']
        print('Total duration: ', df['duration'].sum())
        print('Total file size: ', sizeof_fmt(df['fileSize'].sum()))
        df['fileSize'] = df['fileSize'] * 9.5367431640625e-07
        df.rename(columns={'fileSize': 'fileSizeMB'}, inplace=True)
        df['duration'] = df['duration'].dt.total_seconds().apply(strftd)
        df['year'] = df['dateFrom'].dt.year
        df['month'] = df['dateFrom'].dt.month
        df['day'] = df['dateFrom'].dt.day
        df['hour'] = df['dateFrom'].dt.hour
        df['minute'] = df['dateFrom'].dt.minute
        df['second'] = df['dateFrom'].dt.second

    cols_new += ['query_offset']
    return df[cols_new]


def _list_file_dc(onc, deviceCode, dateFrom, dateTo, statistics):
    """
    Get file list by device code
    """
    returnOptions = 'all' if statistics else None
    filters = {
            'deviceCode'     : deviceCode,
            'dateFrom'       : dateFrom,
            'dateTo'         : dateTo,
            'returnOptions'  : returnOptions
        }

    result = onc.getListByDevice(filters, allPages=True)
    return _api_to_df(result, dateFrom, dateTo, statistics)


def _list_file_lc(onc, locationCode, deviceCategoryCode,
               dateFrom, dateTo, statistics):
    """
    Get file list by location code
    """
    if deviceCategoryCode == 'ask':
        filters = {'locationCode': locationCode}
        results = onc.getDeviceCategories(filters)

        deviceCategoryCode = _ask_options(results, 'deviceCategoryCode', 'deviceCategoryName')

    returnOptions = 'all' if statistics else None
    filters = {
        'locationCode'      : locationCode,
        'deviceCategoryCode': deviceCategoryCode,
        'dateFrom'          : dateFrom,
        'dateTo'            : dateTo,
        'returnOptions'     : returnOptions
    }

    result = onc.getListByLocation(filters, allPages=True)
    return _api_to_df(result, dateFrom, dateTo, statistics)


def _api_to_df(result, dateFrom, dateTo, statistics):
    """
    Extract output from the API
    """
    result = result['files']
    df = pd.DataFrame(result) if statistics else pd.DataFrame(result, columns=["filename"])
    df['dateFromQuery'] = dateFrom
    df['dateToQuery'] = dateTo
    return df


def list_file(onc, deviceCode=None, deviceId=None, locationCode=None, dive=None, deviceCategoryCode='VIDEOCAM',
    dateFrom=None, dateTo=None, quality='ask', extension='mp4', statistics=True):
    """
    Get list of files archived in Oceans 3.0

    Search archived files based on one of the following criterias: decideCode,
    deviceId, locationCode and dive number. One of these parameters need to
    be suplied to the function. 

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    deviceCode : str
        Devide code to seach files
    deviceId : str or int
        Devide Id to seach files
    locationCode : str
        Location code to seach files
    dive : str
        Dive number to seach files
    deviceCategoryCode : str, default VIDEOCAM
        Device category code to search files. Only used when locationCode is suplied.
        Usually 'VIDEOCAM' for fixed cameras and 'ROV_CAMERA' for ROVs.
        'ask' will list avaiable options and ask user to choose one.
    dateFrom : str or datetime
        Return videos after specified datetime. Can be any format that is parsed
        by pandas.to_datetime. If None, will search all videos since the device
        was first deployed.
    dateFrom : str or datetime
        Return videos before specified datetime. Can be any format that is parsed
        by pandas.to_datetime. If None, will search all videos until the current
        date.
    quality : str, default ask
        Especify a quality to filter videos. Usually should be LOW, standard,
        1500, 5000, UHD. 'ask' will list avaiable options and ask user to choose one.
        'all' will get all avaiable videos. Accepts multiple values as comma separated.
    extension : str, default mp4
        Especify a extension to filter videos. 'ask' will list avaiable options and
        ask user to choose one. 'all' will get all avaiable videos.
        Accepts multiple values as comma separated.
    statistics : bool, default True
        Also save video durations and file sizes.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the filenames, and videos duration and file sizes if
        statistics is True
    """
    if dateFrom is not None:
        dateFrom = pd.to_datetime(dateFrom, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    if dateTo is not None:
        dateTo = pd.to_datetime(dateTo, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    if deviceCode:
        df = _list_file_dc(onc, deviceCode, dateFrom=dateFrom, dateTo=dateTo, statistics=statistics)

    elif locationCode:
        df = _list_file_lc(onc, locationCode, deviceCategoryCode=deviceCategoryCode,
            dateFrom=dateFrom, dateTo=dateTo, statistics=statistics)

    elif dive:
        dives = get_dives(onc)

        result = dives[dives['dive'] == dive]
        if result.shape[0] != 1:
            raise ValueError("'dive' argument does not match any dive.")

        if dateFrom is None:
            dateFrom = result['startDate'].values[0]

        if dateTo is None:
            dateTo = result['endDate'].values[0]

        df = _list_file_dc(onc, result['deviceCode'].values[0],
            dateFrom = dateFrom, dateTo = dateTo, statistics=statistics)

    elif deviceId:
        result=onc.getDevices({'deviceId': deviceId})
        df = _list_file_dc(onc, result[0]['deviceCode'], dateFrom=dateFrom,
            dateTo=dateTo, statistics=statistics)

    else:
        raise ValueError("One of {deviceCode, deviceId, locationCode, dive} is required")

    df = _list_file_helper(df, statistics, extension, quality, None)

    return df




def list_file_batch(onc, csvfile, quality='ask', extension='mp4', statistics=True, keep_cols=False):
    """
    Batch list of files archived in Oceans 3.0
    List video files based on parameters stored in a csv file

    This function will execute multiple search for archived files based on parameters stored
    in a csv file. Usefull to get videos from multiple sites and/or dives.
    Column names must be one of decideCode, deviceId, locationCode or dive.
    Csv may also include parameters dateFrom, dateTo, and deviceCategoryCode.
    Details on these parameters can be found in 'list_file' function. A column named 'group'
    can also be used to distingshed each query in the final table.

    Parameters
    ----------
    onc : onc.ONC
        ONC class object
    csvfile : str, path object or file-like object
        Location to the input csv file.
    quality : str, default ask
        Especify a quality to filter videos. Usually should be LOW, standard,
        1500, 5000, UHD. 'ask' will list avaiable options and ask user to choose one.
        'all' will get all avaiable videos. Accepts multiple values as comma separated.
    extension : str, default mp4
        Especify a extension to filter videos. 'ask' will list avaiable options and
        ask user to choose one. 'all' will get all avaiable videos.
        Accepts multiple values as comma separated.
    statistics : bool, default True
        Also save video durations and file sizes.
    keep_cols : bool, default False
        Keep other columns from csvfile into the output.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the filenames, and videos duration and file sizes if
        statistics is True
    """
    file = pd.read_csv(csvfile)
    cols = file.columns.to_list()

    dc = 'deviceCode' in cols
    di = 'deviceId' in cols
    lc = 'locationCode' in cols
    dv = 'dive' in cols

    if (dc+di+lc+dv) == 0:
        raise ValueError("One of {deviceCode, deviceID, locationCode, dive} must be a column \
            in the csv file")

    if (dc+lc) == 0:

        if dv:
            dives = get_dives(onc)

            dives.rename(columns={'startDate': 'dateFrom', 'endDate': 'dateTo'}, inplace=True)
            dives = dives[['dive','deviceCode','dateFrom','dateTo']]

            nrows = file.shape[0]
            file = pd.merge(file, dives, how="inner", on="dive", suffixes=(None,'_y'))

            if file.shape[0] < nrows:
                raise ValueError("Some dives in the csv file do not match any dive.")


        elif di:
            result = pd.DataFrame(onc.getDevices())
            result = result[['deviceCode','deviceId']]
            file = pd.merge(file, result, how="inner", on="deviceId")

        dc = True
        cols = file.columns.to_list() # update

    if not 'dateFrom' in cols:
        raise ValueError("'dateFrom' must be a column in the csv file (exept if 'dive' is provided).")
    if not 'dateTo' in cols:
        raise ValueError("'dateTo' must be a column in the csv file (exept if 'dive' is provided).")

    file['dateFrom'] = pd.to_datetime(file['dateFrom'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str[:-3]+'Z'
    file['dateTo'] = pd.to_datetime(file['dateTo'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str[:-3]+'Z'

    if not 'group' in cols:
        file['group'] = 'row' + file.index.astype(str)

    if keep_cols:
        z = ['group','deviceCode','deviceId','locationCode','dive',
            'dateFrom','dateTo','deviceCategoryCode']
        cols_to_keep = [x for x in cols if x not in z]
    else:
        cols_to_keep = ['fovs'] if 'fovs' in cols else None

    if cols_to_keep is None:
        cols_to_keep_g = ['group']
    else:
        cols_to_keep_g = cols_to_keep + ['group']

    df = []

    if dc:
        for _, row in file.iterrows():
            df_tmp = _list_file_dc(onc, row['deviceCode'], dateFrom=row['dateFrom'],
                dateTo=row['dateTo'], statistics=statistics)
            y = row[cols_to_keep_g].to_dict()
            df_tmp = df_tmp.assign(**y)
            df.append(df_tmp)
    else: #lc
        if not 'deviceCategoryCode' in cols:
            raise ValueError("'deviceCategoryCode' must be a column in the csv file when using locationCode.")

        for _, row in file.iterrows():
            df_tmp = _list_file_lc(onc, row['locationCode'], deviceCategoryCode=row['deviceCategoryCode'],
                dateFrom=row['dateFrom'], dateTo=row['dateTo'], statistics=statistics)
            y = row[cols_to_keep_g].to_dict()
            df_tmp = df_tmp.assign(**y)
            df.append(df_tmp)

    df = pd.concat(df)

    df = _list_file_helper(df, statistics, extension, quality, cols_to_keep)

    return df
