from pathlib import Path
import requests
import pandas as pd

# https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix="B"):
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"

def strftd(nseconds):
    hours, remainder = divmod(nseconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return "{:02.0f}:{:02.0f}:{:02.0f}".format(hours, minutes, seconds)

def parse_time(x):
    return pd.to_datetime(x, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def parse_time_series(x):
    return pd.to_datetime(x, utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str[:-3] + 'Z'


def parse_file_path(arg_input):
    if isinstance(arg_input, pd.DataFrame):
        has_group = 'group' in arg_input.columns
        return arg_input, has_group, True

    path = Path(arg_input)
    url = "https://data.oceannetworks.ca/AdFile?filename="

    if path.is_file():
        if path.suffix == '.csv':
            df = pd.read_csv(path, comment = "#")
            if not 'filename' in df.columns:
                raise ValueError("Input csv must have column 'filename'")
            df['urlfile'] = url + df['filename']
            need_download = True
        else:
            df = pd.DataFrame({'filename': path.name, 'urlfile': str(path)})
            need_download = False
    else:
        directory = path.parent
        p = list(directory.rglob(path.name))

        if len(p) == 0:
            raise ValueError("Input file or folder does not exist.")

        df = pd.DataFrame({'filename': p})
        df['urlfile'] = df['filename'].apply(str)
        df['group'] = df['filename'].apply(lambda x: str(x.relative_to(path.parent).parent))
        df['filename'] = df['filename'].apply(lambda x: x.name)

        if len(df['group'].value_counts()) == 1:
            df.drop(columns='group', inplace=True)
        
        need_download = False

    has_group = 'group' in df.columns
    return df, has_group, need_download


def parse_trim(arg_input, df):
    if isinstance(arg_input, pd.DataFrame):
        if {'dateFromQuery','dateToQuery'}.issubset(df.columns):
            return df
        elif hasattr(df, 'QuerrydateFrom') and hasattr(df, 'QuerrydateTo'):
            df['dateFromQuery'] = df.QuerrydateFrom
            df['dateToQuery'] = df.QuerrydateTo
            return df
        else:
            raise ValueError("Dataframe must have 'dateFromQuery' and 'dateToQuery' attributes when using 'trim'.")
    
    path = Path(arg_input)

    if path.is_file() and path.suffix == '.csv':
        with open(path, encoding="utf-8") as f:
            _ = f.readline()
            x1 = f.readline()
            x2 = f.readline()

        if x1[2:10] == "dateFrom":
            df['dateFromQuery'] = pd.to_datetime(x1[12:-1], utc=True)
            df['dateToQuery'] = pd.to_datetime(x2[10:-1], utc=True)
        else: # batch file was used
            batch_file = Path(x1[14:-1])
            if batch_file.exists():
                file = pd.read_csv(batch_file)

                # configure batch df before merge
                cols = file.columns.to_list()
                if not 'group' in cols:
                    file['group'] = 'row' + file.index.astype(str)
                file['dateFromQuery'] = pd.to_datetime(file['dateFrom'], utc=True)
                file['dateToQuery'] = pd.to_datetime(file['dateTo'], utc=True)
                file = file[['group', 'dateFromQuery', 'dateToQuery']]

                nrows = df.shape[0]
                df = pd.merge(df, file, how="inner", on="group", suffixes=(None,'_y'))

                if df.shape[0] < nrows:
                    raise ValueError(f"Some groups in {batch_file} do not match in {arg_input}.")
            
            else:
                raise ValueError(f"Could not find {batch_file} used to generate the list")

    else:
        raise ValueError("'trim' option is only avaiable when using a csv as input")
        
    return df


def download_file(urlfile, output_file):
    r = requests.get(urlfile, timeout = 10, stream=True)

    if r.status_code == 200 and r.headers["Content-Length"] != '0':
        with open(output_file, 'wb') as f:
            f.write(r.content)
    else:
        with open("log_download.txt", 'a', encoding="utf-8") as f:
            f.write(f"Failed to download file: {output_file}\n")