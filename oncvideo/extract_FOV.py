from pathlib import Path
import tempfile
import subprocess as sp
import pandas as pd
from tqdm import tqdm 
from ._utils import parse_file_path, download_file


def to_timedelta(x):
    if isinstance(x, (int, float)):
        return pd.to_timedelta(x, unit='sec')
    elif ':' in x:
        return pd.to_timedelta('00:' + x)
    else:
        return pd.to_timedelta(float(x), unit='sec')



def extractFOV(arg_input, timestamps=1, duration=None, output='fovs', deinterlace=False):

    fileOut = Path(output + '.csv')
    if arg_input == fileOut.name:
        raise ValueError("Output folder must be a different name than input csv.")

    df, has_group, need_download = parse_file_path(arg_input)

    # make sure timestamps and duration are in the correct format
    if not isinstance(timestamps, list):
        timestamps = [timestamps]

    if not isinstance(duration, list):
        duration = [duration] * len(timestamps)

    if len(timestamps) != len(duration):
        raise ValueError("'Timestamps' and 'duration' must be lists of same length.")

    timeadds = [to_timedelta(x) for x in timestamps]
    timestamps = [str(x) for x in timestamps]
    duration = [str(x) for x in duration]

    # create loginf files
    fovfolder = [f"FOV_{fov.replace(':','-')}" for fov in timestamps]
    header = f"video_filename,{','.join(fovfolder)}\n"
    if has_group:
        header = 'subfolder,' + header
        df.sort_values(['group','filename'], inplace=True)
    else:
        df.sort_values(['filename'], inplace=True)
        df['group'] = 'group1'

    folder = Path(output)
    folder.mkdir(exist_ok=True)

    if deinterlace:
        vf_cmd = ['-vf', 'pp=ci|a']
    else:
        vf_cmd = []

    # check if command has been started alread
    if fileOut.exists():
        tmp = pd.read_csv(fileOut)
        count = df.shape[0]
        df = df[~df['filename'].isin(tmp['video_filename'])]
        count -= df.shape[0]
        f = open(fileOut, "a", encoding="utf-8")
        print(f"{fileOut.name} already exists! {count} files already processed, skipping to remaining files.")
    else:
        f = open(fileOut, "w", encoding="utf-8")
        f.write(header)

    # start for loop
    pbar = tqdm(total=df.shape[0], desc = 'Processed files')

    for name, group in df.groupby('group'):

        if has_group:
            outfolder = folder / Path(name)
            outfolder.mkdir(exist_ok=True)
        else:
            outfolder = folder

        # Create folder for each FOV
        outfolderFOV = []
        for fovf in fovfolder:
            p = outfolder / Path(fovf)
            p.mkdir(exist_ok=True)
            outfolderFOV.append(p)

        for _, row in group.iterrows():
            # extract frames for each video
            file_name_p = Path(row['filename'])
            if need_download:
                tmpfile = tempfile.gettempdir() / file_name_p
                download_file(row['urlfile'], tmpfile)
                tmpfile_str = str(tmpfile)
            else:
                tmpfile_str = row['urlfile']

            # get newname of file
            filename_split = file_name_p.stem.split('_')
            timestamp = filename_split[-1]
            if 'Z-' in timestamp:
                timestamp = timestamp.split('-')[0]

            timestamp = pd.to_datetime(timestamp, format='%Y%m%dT%H%M%S.%fZ', utc=True)
            oldname_dc = '_'.join(filename_split[:-1])

            # Extract frame/video for each FOV
            filename_FOVs = []
            for fov, fov_td, d, p in zip(timestamps, timeadds, duration, outfolderFOV):

                newtime = (timestamp + fov_td).strftime('%Y%m%dT%H%M%S.%f')[:-3]
                filename = f"{oldname_dc}_{newtime}Z{file_name_p.suffix}"
                new_name_p = Path(filename)

                if d == 'None':
                    new_name = p / new_name_p.with_suffix('.jpg')
                    ff_cmd = ['ffmpeg', '-v', 'quiet', '-ss', fov, '-i', tmpfile_str]
                    ff_cmd += vf_cmd
                    ff_cmd += ['-frames:v', '1', '-update', '1',
                        '-qmin', '1', '-qmax', '1', '-q:v', '1', str(new_name)]

                else:
                    new_name = p / new_name_p
                    ff_cmd =['ffmpeg', '-v', 'quiet', '-ss', fov,
                        '-i', tmpfile_str, '-t', d, '-c', 'copy', str(new_name)]

                sp.run(ff_cmd, check=False)
                filename_FOVs.append(new_name.name)
                # rename files to correct timestamp

            if need_download:
                tmpfile.unlink()

            # save csv with video name
            if has_group:
                f.write(f"{name},{row['filename']},{','.join(filename_FOVs)}\n")
            else:
                f.write(f"{row['filename']},{','.join(filename_FOVs)}\n")

            pbar.update()

    pbar.close()
    f.close()
