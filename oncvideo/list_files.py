import pandas as pd
from ._utils import sizeof_fmt, strftd, parse_time, parse_time_series, name_to_timestamp
from .dives_ONC import getDives

def ask_options(results, ivalue, ihelp):
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

def ask_options_multiple(results, ivalue):
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


def list_file_helper(df, statistics, extension, quality):

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
            extension = ask_options_multiple(extn, "extension")
        
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
                quality = ask_options_multiple(qualityn, "quality")

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
    df['start_end'] = ''

    for _, group in df.groupby(cols_sort):

        # first file
        r1 = name_to_timestamp(group['filename'].iloc[0])
        r01 = pd.to_datetime(group['dateFromQuery'].iloc[0], utc=True, format='%Y-%m-%dT%H:%M:%S.%fZ')
        nseconds = (r01 - r1).total_seconds()
        sign = '-' if nseconds < 0 else ''
        df.loc[group.index[0],'start_end'] = f'{sign}{strftd(abs(nseconds))}'

        # last file
        r2 = name_to_timestamp(group['filename'].iloc[-1])
        r02 = pd.to_datetime(group['dateToQuery'].iloc[-1], utc=True, format='%Y-%m-%dT%H:%M:%S.%fZ')
        nseconds = (r02 - r2).total_seconds()
        sign = '-' if nseconds < 0 else ''
        if df.loc[group.index[-1],'start_end'] == '':
            df.loc[group.index[-1],'start_end'] = f'{sign}{strftd(abs(nseconds))}'
        else:
            df.loc[group.index[-1],'start_end'] = f"{df.loc[group.index[-1],'start_end']}/{sign}{strftd(abs(nseconds))}"


    # calculate and print overall statistics
    print('Number of files: ', df.shape[0])
    if statistics:
        cols_new += ['duration','fileSizeMB','year','month','day','hour','minute','second']
        df['dateFrom'] = pd.to_datetime(df['dateFrom'], format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
        df['dateTo'] = pd.to_datetime(df['dateTo'], format='%Y-%m-%dT%H:%M:%S.%fZ', utc=True)
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

    cols_new += ['start_end']
    return df[cols_new]


def list_file_dc(onc, deviceCode, dateFrom, dateTo, statistics):

    returnOptions = 'all' if statistics else None
    filters = {
            'deviceCode'     : deviceCode,
            'dateFrom'       : dateFrom,
            'dateTo'         : dateTo,
            'returnOptions'  : returnOptions
        }

    result = onc.getListByDevice(filters, allPages=True)
    return api_to_df(result, dateFrom, dateTo, statistics)


def list_file_lc(onc, locationCode, deviceCategoryCode,
               dateFrom, dateTo, statistics):

    if deviceCategoryCode == 'ask':
        filters = {'locationCode': locationCode}
        results = onc.getDeviceCategories(filters)

        deviceCategoryCode = ask_options(results, 'deviceCategoryCode', 'deviceCategoryName')

    returnOptions = 'all' if statistics else None
    filters = {
        'locationCode'      : locationCode,
        'deviceCategoryCode': deviceCategoryCode,
        'dateFrom'          : dateFrom,
        'dateTo'            : dateTo,
        'returnOptions'     : returnOptions
    }

    result = onc.getListByLocation(filters, allPages=True)
    return api_to_df(result, dateFrom, dateTo, statistics)


def api_to_df(result, dateFrom, dateTo, statistics):
    result = result['files']
    df = pd.DataFrame(result) if statistics else pd.DataFrame(result, columns=["filename"])
    df['dateFromQuery'] = dateFrom
    df['dateToQuery'] = dateTo
    return df


def list_file(onc, deviceCode=None, deviceId=None, locationCode=None, dive=None, deviceCategoryCode='ask',
    dateFrom=None, dateTo=None, quality='ask', extension='mp4', statistics=False):

    if dateFrom is not None:
        dateFrom = parse_time(dateFrom)

    if dateTo is not None:
        dateTo = parse_time(dateTo)
    
    if deviceCode:
        df = list_file_dc(onc, deviceCode, dateFrom=dateFrom, dateTo=dateTo, statistics=statistics)
    
    elif locationCode:
        df = list_file_lc(onc, locationCode, deviceCategoryCode=deviceCategoryCode, dateFrom=dateFrom,
            dateTo=dateTo, statistics=statistics)

    elif dive:
        dives = getDives(onc, False)

        result = dives[dives['dive'] == dive]
        if result.shape[0] != 1:
            raise ValueError("'dive' argument does not match any dive.")

        if dateFrom is None:
            dateFrom = result['startDate'].values[0]
        
        if dateTo is None:
            dateTo = result['endDate'].values[0]

        df = list_file_dc(onc, result['deviceCode'].values[0],
            dateFrom = dateFrom, dateTo = dateTo, statistics=statistics)

    elif deviceId:
        result=onc.getDevices({'deviceId': deviceId})
        df = list_file_dc(onc, result[0]['deviceCode'], dateFrom=dateFrom,
            dateTo=dateTo, statistics=statistics)

    else:
        raise ValueError("One of {deviceCode, deviceId, locationCode, dive} is required")

    df = list_file_helper(df, statistics, extension, quality)

    return df




def list_file_batch(onc, csvfile, quality='ask', extension='mp4', statistics=False):


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
            dives = getDives(onc, False)

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
    
    file['dateFrom'] = parse_time_series(file['dateFrom'])
    file['dateTo'] = parse_time_series(file['dateTo'])

    if not 'group' in cols:
        file['group'] = 'row' + file.index.astype(str)

    df = []

    if dc:
        for _, row in file.iterrows():
            df_tmp = list_file_dc(onc, row['deviceCode'], dateFrom=row['dateFrom'],
                dateTo=row['dateTo'], statistics=statistics)
            df_tmp['group'] = row['group']
            df.append(df_tmp)
    else: #lc
        if not 'deviceCategoryCode' in cols:
            raise ValueError("'deviceCategoryCode' must be a column in the csv file when using locationCode.")
            
        for _, row in file.iterrows():
            df_tmp = list_file_lc(onc, row['locationCode'], deviceCategoryCode=row['deviceCategoryCode'],
                dateFrom=row['dateFrom'], dateTo=row['dateTo'], statistics=statistics)
            df_tmp['group'] = row['group']
            df.append(df_tmp)

    df = pd.concat(df)

    df = list_file_helper(df, statistics, extension, quality)
    
    column_group = df.pop("group")
    df.insert(0, "group", column_group)
    return df
