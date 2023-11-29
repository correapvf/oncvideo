import requests
# import struct
from pathlib import Path
import io
import numpy as np
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from ._utils import strftd, parse_file_path

def handle_file(urlfile):
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


def read_ddf_info(urlfile):

    f = handle_file(urlfile)

    # https://wiki.oceannetworks.ca/download/attachments/49447779/DIDSON%20V5.26.26%20Data%20File%20and%20Ethernet%20Structure.pdf?version=1&modificationDate=1654558351000&api=v2
    # DDF_03
    filetype = f.read(3)
    if filetype != b'DDF': raise ValueError("File is not a DDF file")
    version = int.from_bytes(f.read(1), "little")
    if version != 3: raise ValueError("Only DDF V3 file supported")

    nFrame = int.from_bytes(f.read(4), "little")
    framerate = int.from_bytes(f.read(4), "little")
    resolution = int.from_bytes(f.read(4), "little") # 1=HF, 0=LF
    NumBeams = int.from_bytes(f.read(4), "little")
    f.seek(4, 1)
    SamplesPerChannel = int.from_bytes(f.read(4), "little")
    nBytes = NumBeams*SamplesPerChannel + 60

    f.seek(484, 1)
    
    time = pd.Series(np.datetime64('now'), index=range(nFrame))
    PCtime = pd.Series(np.datetime64('now'), index=range(nFrame))
    WindowStart = np.empty(nFrame)
    WindowLength = np.empty(nFrame)
    # sonarPan = np.empty(nFrame)
    # sonarTilt = np.empty(nFrame)
    # sonarRoll = np.empty(nFrame)

    for i in range(nFrame):
        f.seek(4, 1)
        dt=int.from_bytes(f.read(4), "little")
        PCtime[i] = datetime.fromtimestamp(dt)
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
        WindowStarti = int.from_bytes(f.read(4), "little")
        WindowLengthi = int.from_bytes(f.read(4), "little")

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
            WindowStartMultiplier = 0.375
            WindowLengthOptions = (1.125, 2.25, 4.5, 9, 18, 36)
        else:  # XW
            WindowStartMultiplier = 0.42
            if rangetype:  # LR
                WindowLengthOptions = (2.5, 5, 10, 20, 40, 80)
            else:  # Std
                WindowLengthOptions = (1.25, 2.5, 5, 10, 20, 40)

        if resolution == 0: # If is LF
            WindowLengthi += 2
            WindowStartMultiplier *= 2

        WindowStart[i] = WindowStarti * WindowStartMultiplier
        WindowLength[i] = WindowLengthOptions[WindowLengthi]

        f.seek(nBytes, 1)

    f.close()

    PCtimeFrom = PCtime.iloc[0].strftime('%Y-%m-%d %H:%M:%S')
    PCtimeTo = PCtime.iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

    sonarTimeFrom = time.iloc[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sonarTimeTo = time.iloc[-1].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    duration_secs = round((time.iloc[-1]-time.iloc[1]).total_seconds())
    duration = strftd(duration_secs)

    WS = ';'.join(np.unique(WindowStart).astype(str).tolist())
    WL = ';'.join(np.unique(WindowLength).astype(str).tolist())

    # PCtimeFrom, PCtimeTo, SonartimeFrom, SonartimeTo, duration, framerate, nBeams, nSamples, windowStart, windowLength
    info = f'{PCtimeFrom},{PCtimeTo},{sonarTimeFrom},{sonarTimeTo},{duration},{framerate},{NumBeams},{SamplesPerChannel},{WS},{WL}'
    return info

def read_ddf(urlfile):
    # function to be used inside python
    f = handle_file(urlfile)

    # https://wiki.oceannetworks.ca/download/attachments/49447779/DIDSON%20V5.26.26%20Data%20File%20and%20Ethernet%20Structure.pdf?version=1&modificationDate=1654558351000&api=v2
    # DDF_03
    filetype = f.read(3)
    if filetype != b'DDF': raise ValueError("File is not a DDF file")
    version = int.from_bytes(f.read(1), "little")
    if version != 3: raise ValueError("Only DDF V3 file supported")

    nFrame = int.from_bytes(f.read(4), "little")
    f.seek(8, 1)
    NumBeams = int.from_bytes(f.read(4), "little")
    f.seek(4, 1)
    SamplesPerChannel = int.from_bytes(f.read(4), "little")
    nBytes = NumBeams*SamplesPerChannel

    f.seek(484, 1)

    out = np.empty((nFrame, NumBeams, SamplesPerChannel), dtype=np.uint8)
    time = pd.Series(np.datetime64('now'), index=range(nFrame))

    for i in range(nFrame):
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

        b = f.read(nBytes)
        out[i,:,:] = np.frombuffer(b, dtype=np.uint8).reshape(NumBeams, SamplesPerChannel, order='F')

    f.close()
    return time, out


def didson_info(arg_input, output='DIDSON_info.csv'):
    df, has_group = parse_file_path(arg_input)

    # configure csv
    header = 'filename,PCtimeFrom,PCtimeTo,SonartimeFrom,SonartimeTo,duration,framerate,nBeams,nSamples,windowStart,windowLength\n'
    seps = ',,,,,,,,,'
    if has_group:
        header = 'group,' + header
        seps = ',' + seps

    fileOut = Path(output)
    if fileOut.exists():
        with open(fileOut, encoding="utf-8") as f:
            count = sum(1 for _ in f)
        count -= 1
        df = df.loc[count:]
        f = open(fileOut, "a", encoding="utf-8")
        print(f"{output} already exists! {count} files already processed, skipping to remaining files.")
    else:
        f = open(fileOut, "w", encoding="utf-8")
        f.write(header)

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):

        to_write = f"{row['group']},{row['filename']}" if has_group else row['filename']

        try:
            info = read_ddf_info(row['urlfile'])
            f.write(f'{to_write},{info}\n')
        except RuntimeError:
            f.write(f'{to_write}{seps}\n')

    f.close()
