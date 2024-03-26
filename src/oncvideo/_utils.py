""" Multiple helper functions used for the package"""
from collections import defaultdict
from pathlib import Path
import requests
import numpy as np
import pandas as pd
import backoff
from tqdm import tqdm
from ffmpeg_progress_yield import FfmpegProgress
from .utils import name_to_timestamp

URL = "https://data.oceannetworks.ca/AdFile?filename="


def sizeof_fmt(num, suffix="B"):
    """
    Convert number to a human readible format
    https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    """
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"


def strftd(nseconds):
    """
    Convert number of seconds to hh:mm:ss format
    """
    hours, remainder = divmod(nseconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:06.3f}"


def strftd2(td):
    """
    Convert timedelta to mm:ss format
    """
    nseconds = td.total_seconds()
    minutes, seconds = divmod(nseconds, 60)
    return f"{minutes:02.0f}-{seconds:02.0f}"


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=3,
    giveup=lambda e: e.response is not None and e.response.status_code < 500
)
def download_file(urlfile, output_file):
    """
    Download a file with a progress bar
    """
    r = requests.get(urlfile, timeout = 10, stream=True)

    if r.status_code == 200 and r.headers["Content-Length"] != '0':
        total = int(r.headers.get('content-length', 0))
        with open(output_file, 'wb') as file, tqdm(
            desc = 'Downloading ' + output_file.name,
            total = total,
            unit = 'iB',
            unit_scale = True,
            unit_divisor = 1024,
            leave = False
        ) as progress:
            for data in r.iter_content(chunk_size=1024*1024):
                size = file.write(data)
                progress.update(size)

        return True
    else:
        with open("log_download.txt", 'a', encoding="utf-8") as f:
            f.write(f"Failed to download file: {output_file}\n")
        return False


def run_ffmpeg(cmd, filename=''):
    """
    Run a ffmpeg command with a progress bar
    """
    ff = FfmpegProgress(cmd)
    with tqdm(total=100, position=1, desc='Processing ' + filename, leave=False) as pbar:
        for progress in ff.run_command_with_progress():
            pbar.update(progress - pbar.n)


def to_timedelta(x):
    """
    Convert number of seconds to pandas.Timedelta object
    """
    if isinstance(x, (int, float)):
        return pd.to_timedelta(x, unit='sec')
    elif ':' in x:
        return pd.to_timedelta('00:' + x)
    else:
        return pd.to_timedelta(float(x), unit='sec')


def trim_group(group):
    """
    Create ss and to paramenters to be passed to ffmpeg
    when trim is needed
    """
    ss = group['query_offset'].iloc[0]
    if ss is not np.nan and '/' in ss:
        ss, to = group['query_offset'].iloc[0].split('/')
        do_both = True
    else:
        to = group['query_offset'].iloc[-1]
        do_both = False

    ss_valid = ss is not np.nan and 'start' in ss
    to_valid = to is not np.nan and 'end' in to

    if to_valid:
        to = to.split(' ')[-1]

    # set filename with the time added
    if ss_valid:
        ss = ss.split(' ')[-1]
        timeadd = pd.to_timedelta(ss)

        ts = name_to_timestamp(group['filename'].iloc[0])
        newtime = (ts + timeadd).strftime('%Y%m%dT%H%M%S.%f')[:-3]+'Z'
        newname = f"{ts.dc}_{newtime}{ts.ext}"

        index0 = group.index[0]
        group.loc[index0, 'filename'] = newname
        group.at[index0, 'skip'] = ['-ss', ss]

        if do_both and to_valid:
            group.at[index0, 'skip'] = group.at[index0, 'skip'] + ['-to', to]

    if not do_both and to_valid:
        group.at[group.index[-1], 'skip'] = group.at[group.index[-1], 'skip'] + ['-to', to]

    return group


def parse_file_path(source, need_filename=True):
    """
    Return a pandas.DataFrame according to the source
    need_filename - check if dataFrame or csv have a filename column
    """
    if isinstance(source, pd.DataFrame):
        if need_filename:
            if not 'filename' in source.columns:
                raise ValueError("Input csv must have column 'filename'")
            source['urlfile'] = URL + source['filename']
        else:
            source['urlfile'] = ''

        has_group = 'group' in source.columns
        return source, has_group, True

    path = Path(source)

    if path.is_file():
        if path.suffix == '.csv':
            df = pd.read_csv(path)
            if need_filename:
                if not 'filename' in df.columns:
                    raise ValueError("Input csv must have column 'filename'")
                df['urlfile'] = URL + df['filename']
            else:
                df['urlfile'] = ''
            need_download = True
        else:
            df = pd.DataFrame({'filename': [path.name], 'urlfile': [str(path)]})
            need_download = False
    else:
        if '*' in source:
            #
            directory = path.parent
            p = list(directory.rglob(path.name))

            if len(p) == 0:
                raise ValueError("No files found matching file pattern.")

            df = pd.DataFrame({'filename': p})
            df['urlfile'] = df['filename'].apply(str)
            df['group'] = df['filename'].apply(lambda x: str(x.relative_to(path.parent).parent))
            df['filename'] = df['filename'].apply(lambda x: x.name)

            if len(df['group'].value_counts()) == 1:
                df.drop(columns='group', inplace=True)

            need_download = False

        else:
            df = pd.DataFrame({'filename': [path.name], 'urlfile': [URL + path.name]})
            need_download = True

    has_group = 'group' in df.columns
    return df, has_group, need_download


def make_names(names):
    """
    Make sure names are unique
    """
    # Create a dictionary to store counts of each name
    name_counts = defaultdict(int)
    unique_names = []

    # Iterate over the cleaned names
    for name in names:
        # If the name already exists, append a sequential number
        if name in name_counts:
            name_counts[name] += 1
            unique_names.append(f"{name}_{name_counts[name]}")
        else:
            name_counts[name] = 0
            unique_names.append(name)

    return unique_names


def create_error_message(response):
    """
    Method to print infromation of an error returned by the API to the console
    Builds the error description from the response object
    """
    status = response.status_code
    if status == 400:
        prefix = f"\nStatus 400 - Bad Request: {response.url}"
        payload = response.json()
        # see https://wiki.oceannetworks.ca/display/O2A for error codes
        msg = f"{prefix}\n" + "\n".join(
            [
                f"API Error {e['errorCode']}: {e['errorMessage']} "
                f"(parameter: {e['parameter']})"
                for e in payload["errors"]
            ]
        ) + "\n"
    else:
        msg = (f"The server request failed with HTTP status {status}.\n"
            f"Request {response.url}\n\n")

    return msg
