"""Read DDF v3 files (DIDSON)"""
import io
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from ._utils import parse_file_path, strftd


def _handle_file(urlfile):
    """
    Return a file connection if string is a URL or a path to a file
    """
    if urlfile.startswith("https"):
        r = requests.get(urlfile, timeout=10)
        if r.status_code == 200 and r.content != b'':
            f = io.BytesIO(r.content)
        else:
            raise ValueError("Could not get URL: " + urlfile)

    else:
        if Path(urlfile).is_file():
            f = open(urlfile, "rb")
        else:
            raise ValueError("Could not found: " + urlfile)

    return f


def _read_ddf_info(urlfile):
    """
    Read headers of ddf v3 file, but skip actual data
    """
    f = _handle_file(urlfile)

    # https://wiki.oceannetworks.ca/download/attachments/49447779/DIDSON%20V5.26.26%20Data%20File%20and%20Ethernet%20Structure.pdf?version=1&modificationDate=1654558351000&api=v2
    # DDF_03
    filetype = f.read(3)
    if filetype != b'DDF':
        raise ValueError("File is not a DDF file")
    version = int.from_bytes(f.read(1), "little")
    if version != 3:
        raise ValueError("Only DDF V3 file supported")

    nframe = int.from_bytes(f.read(4), "little")
    framerate = int.from_bytes(f.read(4), "little")
    resolution = int.from_bytes(f.read(4), "little") # 1=HF, 0=LF
    num_beams = int.from_bytes(f.read(4), "little")
    f.seek(4, 1)
    num_samples = int.from_bytes(f.read(4), "little") # samples_per_channel
    nbytes = num_beams*num_samples + 60

    f.seek(484, 1)

    time = pd.Series(np.datetime64('now'), index=range(nframe))
    pctime = pd.Series(np.datetime64('now'), index=range(nframe))
    window_start = np.empty(nframe)
    window_length = np.empty(nframe)
    # sonarPan = np.empty(nframe)
    # sonarTilt = np.empty(nframe)
    # sonarRoll = np.empty(nframe)

    for i in range(nframe):
        f.seek(4, 1)
        dt=int.from_bytes(f.read(4), "little")
        pctime[i] = datetime.fromtimestamp(dt)
        f.seek(12, 1)

        # f.seek(20, 1)
        time[i] = pd.Timestamp(
            year=int.from_bytes(f.read(4), "little"),
            month=int.from_bytes(f.read(4), "little"),
            day=int.from_bytes(f.read(4), "little"),
            hour=int.from_bytes(f.read(4), "little"),
            minute=int.from_bytes(f.read(4), "little"),
            second=int.from_bytes(f.read(4), "little"),
            microsecond=int.from_bytes(f.read(4), "little")*10000
        )
        f.seek(4, 1)
        window_start_i = int.from_bytes(f.read(4), "little")
        window_length_i = int.from_bytes(f.read(4), "little")

        # f.seek(100, 1)
        # sonarPan[i] = struct.unpack('f', f.read(4))[0]
        # sonarTilt[i] = struct.unpack('f', f.read(4))[0]
        # sonarRoll[i] = struct.unpack('f', f.read(4))[0]
        # f.seek(20, 1)

        f.seek(132, 1) # comment if above is uncommented

        b = f.read(4)
        windowtype = b[0] & 0x1 # 1=classic, 0=extended windows
        rangetype = (b[0] >> 1) & 0x1 # 0=Standard, 1=LR

        if windowtype:  # CW
            window_start_multiplier = 0.375
            window_length_options = (1.125, 2.25, 4.5, 9, 18, 36)
        else:  # XW
            window_start_multiplier = 0.42
            if rangetype:  # LR
                window_length_options = (2.5, 5, 10, 20, 40, 80)
            else:  # Std
                window_length_options = (1.25, 2.5, 5, 10, 20, 40)

        if resolution == 0: # If is LF
            window_length_i += 2
            window_start_multiplier *= 2

        window_start[i] = window_start_i * window_start_multiplier
        window_length[i] = window_length_options[window_length_i]

        f.seek(nbytes, 1)

    f.close()

    pc_time_from = pctime.iloc[0].strftime('%Y-%m-%d %H:%M:%S')
    pc_time_to = pctime.iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

    sonar_time_from = time.iloc[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sonar_time_to = time.iloc[-1].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    duration_secs = round((time.iloc[-1]-time.iloc[1]).total_seconds())
    duration = strftd(duration_secs)

    ws = ';'.join(np.unique(window_start).astype(str).tolist())
    wl = ';'.join(np.unique(window_length).astype(str).tolist())

    # pc_time_from, pc_time_to, SonartimeFrom, SonartimeTo, duration,
    # framerate, nBeams, nSamples, windowStart, windowLength
    info = (f'{pc_time_from},{pc_time_to},{sonar_time_from},{sonar_time_to},'
        f'{duration},{framerate},{num_beams},{num_samples},{ws},{wl}')
    return info

def read_ddf(urlfile):
    """
    Get data from DIDSON file

    Parameters
    ----------
    urlfile : str
        Path to a file or a URL.

    Returns
    -------
    numpy.array
        A 1d array with the timestamp for each frame.

    numpy.array
        A 3d array as [frame, Beams, Samples].
    """
    f = _handle_file(urlfile)

    # https://wiki.oceannetworks.ca/download/attachments/49447779/DIDSON%20V5.26.26%20Data%20File%20and%20Ethernet%20Structure.pdf?version=1&modificationDate=1654558351000&api=v2
    # DDF_03
    filetype = f.read(3)
    if filetype != b'DDF':
        raise ValueError("File is not a DDF file")
    version = int.from_bytes(f.read(1), "little")
    if version != 3:
        raise ValueError("Only DDF V3 file supported")

    nframe = int.from_bytes(f.read(4), "little")
    f.seek(8, 1)
    num_beams = int.from_bytes(f.read(4), "little")
    f.seek(4, 1)
    num_samples = int.from_bytes(f.read(4), "little")
    nbytes = num_beams*num_samples

    f.seek(484, 1)

    out = np.empty((nframe, num_beams, num_samples), dtype=np.uint8)
    time = pd.Series(np.datetime64('now'), index=range(nframe))

    for i in range(nframe):
        f.seek(20, 1)
        time[i] = pd.Timestamp(
            year=int.from_bytes(f.read(4), "little"),
            month=int.from_bytes(f.read(4), "little"),
            day=int.from_bytes(f.read(4), "little"),
            hour=int.from_bytes(f.read(4), "little"),
            minute=int.from_bytes(f.read(4), "little"),
            second=int.from_bytes(f.read(4), "little"),
            microsecond=int.from_bytes(f.read(4), "little")*10000
        )
        f.seek(208, 1)

        b = f.read(nbytes)
        out[i,:,:] = np.frombuffer(b, dtype=np.uint8).reshape(num_beams, num_samples, order='F')

    f.close()
    return time, out


def didson_info(source, output='DIDSON_info.csv'):
    """
    Get information from DIDSON file

    Include file timestamp, duration, fps, number of beams and samples,
    window start (distance from DIDSON to first sample) and window length
    (distance from first sample to the last).

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *)
    output : str, default 'DIDSON_info.csv'
        Name of the csv file to save video information
    """
    df, has_group, _ = parse_file_path(source)

    # configure csv
    header = ('filename,PCtimeFrom,PCtimeTo,SonartimeFrom,SonartimeTo,'
        'duration,framerate,nBeams,nSamples,windowStart,windowLength\n')
    seps = ',,,,,,,,,'
    if has_group:
        header = 'group,' + header
        seps = ',' + seps

    file_out = Path(output)
    if file_out.exists():
        with open(file_out, encoding="utf-8") as f:
            count = sum(1 for _ in f)
        count -= 1
        df = df.loc[count:]
        f = open(file_out, "a", encoding="utf-8")
        print(f"{output} already exists! {count} files already processed,"
            "skipping to remaining files.")
    else:
        f = open(file_out, "w", encoding="utf-8")
        f.write(header)

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):

        to_write = f"{row['group']},{row['filename']}" if has_group else row['filename']

        try:
            info = _read_ddf_info(row['urlfile'])
            f.write(f'{to_write},{info}\n')
        except RuntimeError:
            f.write(f'{to_write}{seps}\n')

    f.close()
