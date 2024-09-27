""" Multiple helper functions used for the package"""
from collections import defaultdict
from string import Template
from pathlib import Path
import requests
import numpy as np
import pandas as pd
import backoff
from tqdm import tqdm
from ffmpeg_progress_yield import FfmpegProgress
from .utils import name_to_timestamp

URL = "https://data.oceannetworks.ca/AdFile?filename="

class DeltaTemplate(Template):
    delimiter = "%"

def strfdelta(tdelta, fmt):
    """
    A custom function to convert timedelta object to string with format
    """
    d = {}
    l = {'y': 31536000, 'm': 2592000, 'w': 604800,'d': 86400, 'H': 3600, 'M': 60, 'S': 1}
    f = {'y': '{:d}', 'm': '{:d}', 'w': '{:d}', 'd': '{:d}', 'H': '{:02d}', 'M': '{:02d}', 'S': '{:02d}'}
    rem = int(tdelta.total_seconds())

    for k in ('y', 'm', 'w', 'd', 'H', 'M', 'S' ):
        if f"%{k}" in fmt or f"%{{{k}}}" in fmt:
            tmp, rem = divmod(rem, l[k])
            d[k] = f[k].format(tmp)

    t = DeltaTemplate(fmt)
    return t.substitute(**d)



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


def strftd2(td, div='-'):
    """
    Convert timedelta to mm:ss format
    """
    nseconds = td.total_seconds()
    minutes, seconds = divmod(nseconds, 60)
    return f"{minutes:02.0f}{div}{seconds:02.0f}"


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


LOGO = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x01\x11\x00\x00\x01\x11\x08\x02\x00\x00\x00\xd3\xf6\x04\xd9\x00\x00\x00\tpHYs\x00\x00\x05\x89\x00\x00\x05\x89\x01mh\x9d\xfa\x00\x00\x0e\x07IDATx\x9c\xed\xddOH\x1cw\x1f\xc7\xf1\xe9\xc3\xb2\x152UY\x03\x1aR\xb5\xcf\x13\x17\x03*Az\xb0`l\x1eh/\xf1\x10z\x8c%\xb9<\x97\x04\x92s\x02\xf6\\y\xf2\x9c\x95\'=<\xcf%\x05s\x0c9\x98K\x1ehk=x\xc8\x13\x82\n\t6O\xab&\xc4@\x14c\xb6`\x97=<\x87)\xb2\x9d\x7f;\x9f\xdf\xcc\xec\xec\xba\xef\x17\xb9dt\x7f\xfevw>\xbb3\xbf\xf9\xcd\xf7\xf7\xde\xdf\xff\xfb\xb3\x05 \x9a\xb3=\x9d\xb9\x7f<\xde\xc8\xba\x1b@\xf3\x18\xb5\xfe\x94u\x17\x80&Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00M\xaen\x7f\xa9\xcfn\xeb\xfd\xa0\xad\xe6\xaf\xed\xffVY\xd9-%\xd5\x9ac\xeb\xdd\xc1f\xe9\xc0\xf7G\x1d\xf9\xdcp\x97\x1d\xb1\x9d\xf0\xbe\x8d\x9f\xe8\xacggj\xb6\x86\x94\xd4)3S\xc5\x9e\xd9\x89\xc1\x88\xbf|w\xfd\xf5\xb5\xc5\xa7I\xb5fY\xd6~\xb9\xf2\xe7o\x97\xbc\xdbG\n\xf6\xfd\xc93\xedy\xe1E\xe8\xfa\xf7\xf7\xde\x8d\x1d\xf9\xdc\xfd\xc93\xc3\x85\xa8\xbb\xfb\xf5\xc5g\xf3\xeb\xdb\xde\xeds\x9f\x0e\x9e\xef;\x1e\xbd3\xe1\xad!%\xf586Sw\xf1\x8b\xc5\xee\xb9\x89\xd3I\xb5fY\x96o*\x0c\x02\xe3K\r\x8ceY\xb3\x13\x83S\xc5\x1e\xefv\xb3\xce\x04\xb5\x86\x94\xa4\x9e\x19\x83]\xdc\n\x8e\x8dYk^\x19\x06\xc6\x91\xec\x8e>;1836\x90Tk\x08\x97nf\xc6Ot\x1a\xef\xe2\x17\x8b\xdd7G?\xaa\xde2R\xb0\x13\t\x8ceY\x89\x04\xc6\xb2\xac\x99\xb1\x01\x83\xc08f\'\x06\xfb\xec\xa8\xa7d5]\x19:\xc9\xb7M}\xa4\x9b\x99\xb3=\x91N\x8b\x83\x8c\x9f\xe8\xa8\xfe\xefd\xbf|\xac\x1f\xd0lg"\x81\xb1,\xab\xf7\x83\xf7\xe3=<\xb1\xccX\x96\x95`\x02\x11\xa2~\xe3f\x19Z\x8d0\x10\xb7_\xae=^\xb7_\xaeDi\xe7\xf6\xdaK\xef\xf6\x91\xaec\x06\xe7\xf7h@\x19d\xe6\xaf\xf7\x1ey\xf7\xce\x8e|\xee\xfb/>\xee\xd5?)}[3pm\xf1\xd9\xc2\xc6\x9b\xf8\xed\xac\xec\x96n=\xfe\xc5\xbb}\xfcD\xa7Af\xb6J\x07\xe7\xee=z\xeb\xc9\xeaH\xc1\xfe\xee\x8b\x8f\xcdz\x88\x982\xb8\xa6\xe9\xbb\x8b\xbf-W\xcc\xae3$\x12\x18\xa7\x03\x89\xb4\x93\xac\xcd\xd2\x81o\xc7\x92z\xd60\xc0<\x00@\xd3\x12\xe73Q|9Pc\xd0i\xe9\xd5^}z\x82\x06Gf~w\xb1\xd8}\xb1\xd8\x1d\xf2\x0b\x17\x1e<!6\xb0\xc8L\xe2F\n\xb6\xeb\xb2\xd2\xef\xdb\xbb\x8e\xd5\xbd/H\x05\x99IX{>wc\xb4?\xeb^ E\x8c\x01\x00\x1a2\x13\xc9~\xb9\xb2\xf5\x8e)\xf7\xb0,\x8e\xcd\x0e]\xfe\xcfZ"\xd74q\xe4\xf1=\xf3\xbb\xc6\xbc\xa6\x89\x06\xc4\xf7L\xc2\xb6J\x07\xd7\x16\x9fy\xb7\x9f\xed\xe9dl\xe0h\xc8 3W\x87>\xf4\x9do6b4\xa9\xde\xb75\x97(\xf7K\x8f\x14\xecD.\xbfl\x96\x0e\x82\xda\xb9a\x91\x99\xa3 \x83\xcc|=v\xaa\xfe\xad\xf9\xde\x93\xecj\xa7fSA\xf7H\xa3\xa5\xa4{>\x93\xecIBR\xad\xed\xfff\xd8NRw\xdd\xa0\xa9\xa5\x9b\x99\x85\x8d7Q\xee9\t\xe2\xba\x11e~};\xca\x9d05\xad\xec\x96\x96\xb6\x99\x05\x03C\xe9ff\xb3tpa\xe1\x89Yl\xae{nhy[\xae\\Xx\x92Hl.?\\K\xa4\x1d\xb4\xa0\xd4\xc7\x9aWvK\x06\xb1\t\xaa?\x94Tl\x12\x8c\x1fZM=\xae\xcf8\xb1\xd9\x8avK\xd9~\xb9\x12^\xb0\xcb\xd9\xdd\xa5\x83+\xdfl8\xed\xdc]\x7f\x1d\xbd\x1do\xf2\xbd[Vw~\xf5}\xec\xd6\xbb\x03\x9f\x87\xff\xf1\xcc\xca\xfb\xd8\x90\xcf\x1a\xef\x93\xe2\x12S}\xbcW\xf8\xd7wY\xf7\x01h\x1a7F\xfb\x99\x07\x00h\xc8\x0c\xa0!3\x80\x86\xcc\x00\x9at/lO\xf6\x1f\xbf\xf3\xd9\x90\xfa\xa8o\xd6^N/\xff\x14\xf2\x0b#\x05\xfb\xce\xe7C\xd1\x8b\xa1U\xcfy1\xeb\x92\xab\x91\xabC\x1f\xba&\xdaT\xff\xd4[|\xec\xc1\xe6\x9bK\x0f\xd7B\x1e{\xee\xde#\xdfRU3c\x03W\x86NF\xe9\xdeV\xe9\xe0\xf2\xc35\xdfiu?_\x1aWg0,m\xef]~\xb8\xe6;\x10\xe7}v!\xfd\x0f\xaag]\xfd\x82\x1c\x92\x8aq\xfb\xb6P\x1f\xe9~\xcf\x98M\xbb\xbc2t2d]\x00\xa7<\xb9T=\xb0z\x8f1\xeb\x92\xab\x91\x0e\xcf.X\xfd\xd3\xf6\xf7\xc3~\xea\xfbX\xdf"\xb4s\x13\xa7#\x06\xc6\xb2\xac^\xbb\xed\xfe\xe4\x19\xdfgg0\xe5g\xbc\xa7\xf3\xfe\xe4\x19oW\xad\x80g\xe7\xdb\xff\xa0\xc0\xac\xee\x96\xae\xfd\xe0\x9e\xfa\xadV\xaf\xcfp\x1eS\x83\x1e\x9b\x05\xad\x0b\x90T=\xff\xc677q:\xbc\x0e\x8eW{>\x17\x14\x1b\x03\xc3\x05;(6\x11\xf9\x16\x80\xdf/W\xae\xff\xf0\xcc\xf5\r\x96\xd4r\x0f\xf5\xd1\xa0\x99\xb1\xfc\xd6\x05p>\xb7Z!03c\x03j`\x1cNl\x92\xea\xc6p\xc1\xbe\xf3\xb9\xc9q\xac\x15\xfc\x14.,<q\x1d@N\xf6\x1fo\xa2\xc0X\x8d\x9c\x19\xcb\xb3.\xc0p\x97\xdd\n\x81\xb1,+\xfa!\x99W{>\x17q\x95\xc2(\xc6\x8dVv\x98*\xf6\xf8>\x85\xeb\x8b\xcf\xbcg\\Wc<\xd9L4tf\x92R=\xcd\xc4x\x82I\x9c\t\xda-%h\xd1\xa1o\xd6^&\xb8\x86a\xd0\x1c\xa5:\xc8\xe0c\xfb\xab\xe5\xe7\xbe\xc3;s\x13\x83\x06\xeb\x02\xa8\xb5/n\xaf\xbdp\xfd\xf5\x8e|\xce;\x92\xf6`\xf3\xcd?\xffx\'B\xb6ugVwK\xd3\xcb\xcf\xbd\xdb\xbf\x1c\xe818\x8a\x0bZL!\xfa0]\x90\x91\x82\xfd\xad\xdf\xb0\xe4\xdd\xf5\xd7\xe1c\xa1\xd5\xfe\xf2\xedR#\xcf\x9d\xcb 3\xb7\xd7^\xf8n\xdf,\x1d\x18d\xc6\xe0\xc5\x8dr\x0f\xf3\xca\xce\xaf\rUivac\'\xa8?\x06\x99\t\xba\xd3;\xe6\x9e\xda\x91\xcf\xcd~:\xe8=~^\xdd-E\x0fL\xfcn\xa4\xad%\x8e\xcdP\x1f\xbe#\xcb\xfb\xe5\xca\x85\x85\'\r\x1e\x03I\xd3g&\xa9\xa1U\xc447q:\xa9\xc04\xf8{\xda\xf4\xc3P5k_$\xb5\x10\x1aB\xdc\x1c\xed\xf7\x1da\x9b\x0e8w\r\x17\xbe\x84[\xf8\xedUu\xd0\xf4\x99\xa9\xc9{\xdd\x1aA\x05\xae\x8cW\xb1\r\x1a\x92\x1e)\xd8\xf3\xb5\x1e\xbb\xf4\xea\xad4\xa2\xed\x0c\xcae\x18\x1b\xf6\xa7V\x94l\xb9\xac\x10W\x86N\xce\xafo\x87\x7f\xd5\xdcz\xfcK\x9f\xdd&\x8ddd\x1b\x9b\xa6?\x9fA\xaa"\xde\x91\x1e\xe2\xebOj\xe7\xf3\xda\xe2S\xe9&s\xcb\xb2f\'\x06\x13\xbct+9\xfa\x991\xaef\x86\xfdr\xe5r\xec\xb9\xc3\xe3=\x9dW\x87>\xac\xf9k\x06\xb19k4G!\xbe\xa6?6\xfbj\xf9y\xd0\x05\x1f\xc4\xe1\x0cyIg\xf0\xce\x85\xd7\xfb\xe7\xdd\x13\xden\x8e\xf6\xcf\xafo\xd7\x1c=\xbb\xb6\xf8\xf4\xf6\xda\x8b\xa0\xf3\xcf\xc6)x\xdd\xf4\x99aL,\r\x06\x81q&,\xaf\xec\x96\xee\xae\xbfv\x9d\x9c\xb4\xe7s3c\x03\xd7\x16\x9f\xd6l$\xfc/6H\xc1\xeb\xa3\x7fl\x06\xaf\xfdrei{\xcf\xf9\xe7{\xc6\xb2Y:P\x03s\x98\xb1\xe9\xe5\x9f\xbcs\xf3.\x16\xbb\xb3:\xfdH\\\xd3g&\xce\r\x1e\xc9:\xbc\x12\xe7\xbd$\xd7g\xb7\x1d\xf6\xd3l1\xda\xa0G\x99]\xfe\xbb\xbd\xf6\xf2\xc2\xc2\x13\xe7\xdf\xb9{\x8f\xbc\xbb\xf8p\xc1\x9e*\xd6X\xfc\xbdZ\xf5u\x98\xb7\xe5\x8a\xabh\xb0c.\xde\x84\xff\xc6y\xa3\x1b\xa5\x1f\xc6\xa2\xdc\xa8\xbc\xba[:w\xefQ\xda=\t\xb9\x12\xd7k\xb7\xfd\xef\xd2x\x9c\xc6\xcf\xf7\x1d\xdf\xf9\xdb\xb98-\x04y[\xaeL/?\xf7\xceD\x9e\x19;\xb5\xb0\xf1&\xe2%|\xd7\x8d\xcd\xb7\xd7^L\x15\xbb]\xb3\x07{\xed\xb6\x9b\xa3\x1f\xddz\xfc\x8b\xeb\xb1S\xc5\x9e\xa9\x08\x03\xcd\x8d39\xa0\xe93\x13\x85wNG\xa3\xc9vpo~}\xfb\xea\xd0I\xd7\xab\x14\xfd$\xc4\xebm\xb9r\xeb\xf1\x867\x87W\x87N\xce\xafo\xbb\x026U\xec6\xbbK\'+\xcdtl\xb6\xbaSj\x90\x9bX\x92]ysu\xb7\xe4:yPG]\xab\xed\x97+\xab;\xf2\xb8\x88\xef\x8d\x06qNB|\x17qh\xcf\xe7f?M\xec\x96\xcc\x1f3Z\xdc\xa1\x992\xe3TXn\x84\xd8\xac\xec\x96\xae\xfb-\x00h`u\xb7ta\xe1\x89k\xe3\xf4\xf2Of\xf5\xd7\x8d\'\x11/\xbd\xda\xf3\r\xeaL\x8c\x19\x03\xbe9\x1c\xef\xe9\x9c\xec?n\xdc\xe6\xa1\xeb\x8b\xcf\xb2\xbaY\xa3\x992c\x99\xae2\x90\x86\xf9\xf5\xed\xf8\xb1q\x02\xe3\xdd\xc5\xcd\x96-0\x18 \xae\xe6;\xde5\\\xb0\xa3\\\x91\xf4\xb5\xf4j\xcf\xb7\x14\xfd\xcc\xd8\xa9\x98\'\xf4\xd9N\xd3\xac\xf7:g!\xbb{\x94\xaa\xfb\x96\xb8\xca@\xcd?\x1a\xf4\x0bQ>\xaa\x9d\xd8\x18\x078(0\x87\x1d\x90\x96?\xd8*\x1d\x04\x05&\xe2\xb3sNB\xbc\xdbo\xfe\xf1J\xa2\xef\xa9W\xd0\xf9\xd8\xf5\x1f|^\x9f^\xbb\xadzP.\xa9uV\xea\x86u\x01\xe2\xea\xc8\xe7\x86\xbb\xe41\x86(\xcb\xe2:F\nv\x94\xa9\xd9I\x1d\xa8\xf4\xd9m\xaebe\xd1\xbb\xea\xcb\xf7\xf5Y\xdd)\x1d\xe6Vz\x01\xab\x1f\x98\x89\x1b\xa3\xfdd\x06\x10\xb0\x96\x06 #3\x80\x86\xcc\x00\x9a:\xcd\x03\x18)\xd8_\x7fr*\xfa\xe5\xde\x90\xa5\x01"\xd6\xe0\xda/W\xe6\x83kj}\xff\xc5\xc7\xea\xe4\x00\xa7BW\xf8\t\xe8T\xb1\xe7\xe6h\x7f\x94\x8aS!\x85\xf7\xbd\x0b\x07\xd4\xb4\xba[r\xe6\x14\x87\xfcN\xf4\xb7`\xabt0\xbd\xfc<\xd9\xeb\xb6GI=\xbeg\x9c\xc2\xe4\xd2\xfc\x88\xe1\x80)\x89\xd1K\xe5\xb7\xe7s!\xeb\x0b\x18\xcc\xa6\xb9X\xec\x0e\xaf\xf9\xed\x14\xea\x8eX\xa2-\xa4\xf0\xbe\xc1\xb5\x0b\xa7\x1ey\xc8\x8c,\xe9-\xe8\xb5\xdb\xee|6$\xcd\xd1l)\xa9g&\xc1J\xfe\x06\xa5\xf2\x83\xd6\x170\x13R*\xdf\xa0\xb2}\xfc\xc2\xfb\xd5B\x16\x050{\x0bf\'\x06\x89\x8d\xaft3\xd3g\xb7e\x18\x18\xc7\xc5b\xf7\xcc\xd8@\xfc\x0e8\x9c\x1d\xdd\xb5\xd1\xb8\xb2}\x1a\xb1q\xd5\x8e\x89\xf3\x16\xccN\x0c&2\xcf\xe5\x88I73\x93\xfd\xc7\x13\t\xccH\xc16\x0b\x8c#f\rb\x97\xe1\x82\xed\xfa8\x8fS\xd9~\xb8`\'\xb8_\xb6\xe7s\xae/\x87\x98o\xc1\x971^\xf6\xa3*\xdd\xcc$\xf6\t\xda`5\xca\x92\xed\x8fqU\xb1(b\xbe\x05-\xb2x\x89$\x83W\xe4\x9b\xb5\x975\xa7?D\x19\xb4\t\xaa\x9eaV\xdc~u\xb7\xb4\xb0\xb1\xe3\xdd>\xd9\xdfe0`\x10t\x97\x9bw1\xca\x88|_\xb4\x8e|\xce\xec+4\xa8\xf0\xbe\xc1pb\x0b\xca 3R\x89\xf8\x10\xc9\x16\xb7_\xd8\xd8\xf1\xdeBhY\xd6\x8f\xdb{\xdeB*5\x05\xf5!\xce\xa4c\xdf\xedf\x99\t\x99\x18j\xd0Z\xab\xe1\x9a&\xa0!3\x80\x86\xcc\x00\x9a&\xceL\xd0eo\xb3bH\xc9\n\x1a\xadj\x90\xe2)\xbe\xdd\xe8\xc8\xe7R\x1d\xc1;2\x9ax$\xb1\xe6\xca3\x19\x1a.\xd8)\x95VJ\x84\xd9\xd8\x1d\x1cM\xfc=\x03d\x82\xcc\x00\x1a2\x03h\xc8\x0c\xa0i\xe21\x80\xa0\xd9.W\x87N6\xc2,)\xdf\xd9.#]\xc7\xce\xf7e?S\xf8\xc1\xe6\x9b\x95\x9d_]\x1b\x8dg\xe2\xb4\x9a\x0c\xf6\xad\x8e|.\x919\x1a\xd3\xcb\xcf\x83\n\x14e\xbe\xb8\xcf\xd2\xf6^\xd0l\x17\xb3\xf14\xdf\x17\xcdx\xfe\xe5\xa5\x80\xd5\xcb\x86\xbb\x8e5W\xe9\xe4Ld\x90\x99(\x15\xf2\x1fl\xbe\tz_[S\xcce\x05\x90\xa0\x06=\x9fi\x84\x83+\xc0W\xba\x99\xc9\xaar{+\xdb\x8c\xbd\xd22\xc2\xa5\x9b\x99\xa5W{I\xd5\xcfG\x14w\xd7_g[\xcb\xb8\x15\xa4~l\x96H\xfd|Dqw\xfd\xb5\xd9\x12K\x90\xd4\xe3|\x86\xd8\x84Kd\x14\x91\xc0\xd4M\xfdj\x9c{\x0b\xce\x87\xd8zw\xe0:.\xf7.\xb8\x154\xd0\xec\xad3\xef\xadl\xef-\xb6\x1fRp\xbe\xe6\x9f\xf6>5o\xffC\xfe\xb4\xab5\x83\x85\x06BJ\xf7Gy5\x0eIO\xa45\xb1.\x00\xa0a]\x00@Ff\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@Cf\x00\r\x99\x014d\x06\xd0\x90\x19@\xf3\x7f1\x03\x8e\x0cPu\xb8\xa9\x00\x00\x00\x00IEND\xaeB`\x82'
