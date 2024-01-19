from pathlib import Path
import requests
import pandas as pd
import backoff
from tqdm import tqdm
from ffmpeg_progress_yield import FfmpegProgress

URL = "https://data.oceannetworks.ca/AdFile?filename="

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
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:06.3f}"

def parse_time(x):
    return pd.to_datetime(x, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def parse_time_series(x):
    return pd.to_datetime(x, utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str[:-3] + 'Z'


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=3,
    giveup=lambda e: e.response is not None and e.response.status_code < 500
)
def download_file(urlfile, output_file):
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
        ) as bar:
            for data in r.iter_content(chunk_size=1024*1024):
                size = file.write(data)
                bar.update(size)
        # with open(output_file, 'wb') as f:
        #     f.write(r.content)
        return True
    else:
        with open("log_download.txt", 'a', encoding="utf-8") as f:
            f.write(f"Failed to download file: {output_file}\n")
        return False


def run_ffmpeg(cmd, filename=''):
    ff = FfmpegProgress(cmd)
    with tqdm(total=100, position=1, desc='Processing ' + filename, leave=False) as pbar:
        for progress in ff.run_command_with_progress():
            pbar.update(progress - pbar.n)


def name_to_timestamp(filename):
    r1_split = filename.split('_')[-1].split('.')
    if '-' in r1_split[1]:
        r1_split[1] = r1_split[1].split('-')[0]
    r1 = f'{r1_split[0]}.{r1_split[1]}'
    r1 = pd.to_datetime(r1, format='%Y%m%dT%H%M%S.%fZ', utc=True)
    return r1


def trim_group(group):
    if '/' in group['start_end'].iloc[0]:
        ss, to = group['start_end'].iloc[0].split('/')
        do_both = True
    else:
        ss = group['start_end'].iloc[0]
        to = group['start_end'].iloc[-1]
        do_both = False

    ss_valid = not '-' in ss
    to_valid = not '-' in to

    # set filename with the time added
    if ss_valid:
        timeadd = pd.to_timedelta(ss)

        r1_split = group['filename'].iloc[0].split('_')
        r1_split2 = r1_split[-1].split('.')

        if '-' in r1_split2[1]:
            timeZ, bitrate = r1_split2[1].split('-')
            newtime = pd.to_datetime(f'{r1_split2[0]}.{timeZ}', format='%Y%m%dT%H%M%S.%fZ', utc=True)
            newtime = newtime + timeadd
            newtimestr = newtime.strftime(format='%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
            newname = f"{'_'.join(r1_split[:-1])}_{newtimestr}-{bitrate}.{r1_split2[2]}"
        else:
            newtime = pd.to_datetime('.'.join(r1_split2[:-1]), format='%Y%m%dT%H%M%S.%fZ', utc=True)
            newtime = newtime + timeadd
            newtimestr = newtime.strftime(format='%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
            newname = f"{'_'.join(r1_split[:-1])}_{newtimestr}.{r1_split2[2]}"

    if ss_valid:
        index0 = group.index[0]
        group.loc[index0, 'filename'] = newname
        group.at[index0, 'skip'] = ['-ss', ss]

        if do_both and to_valid:
            group.at[index0, 'skip'] = group.at[index0, 'skip'] + ['-to', to]

    if not do_both and to_valid:
        group.at[group.index[-1], 'skip'] = group.at[group.index[-1], 'skip'] + ['-to', to]

    return group


def parse_file_path(arg_input):
    if isinstance(arg_input, pd.DataFrame):
        has_group = 'group' in arg_input.columns
        return arg_input, has_group, True

    path = Path(arg_input)

    if path.is_file():
        if path.suffix == '.csv':
            df = pd.read_csv(path, comment = "#")
            if not 'filename' in df.columns:
                raise ValueError("Input csv must have column 'filename'")
            df['urlfile'] = URL + df['filename']
            need_download = True
        else:
            df = pd.DataFrame({'filename': [path.name], 'urlfile': [str(path)]})
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
