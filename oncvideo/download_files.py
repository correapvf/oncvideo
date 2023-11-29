import subprocess as sp
import tempfile
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from ._utils import parse_trim, download_file

def parse_file(arg_input):
    # This is a simpler version of parse_file_path
    if isinstance(arg_input, pd.DataFrame):
        has_group = 'group' in arg_input.columns
        return arg_input, has_group

    path = Path(arg_input)
    url = "https://data.oceannetworks.ca/AdFile?filename="

    df = pd.read_csv(path, comment = "#")
    if not 'filename' in df.columns:
        raise ValueError("Input csv must have column 'filename'")
    df['urlfile'] = url + df['filename']

    has_group = 'group' in df.columns
    return df, has_group


def download_files(arg_input, output='output', trim=False):

    df, has_group = parse_file(arg_input)

    if trim:
        df = parse_trim(arg_input, df)

    if has_group:
        df.sort_values(['group','filename'], inplace=True)
    else:
        df.sort_values(['filename'], inplace=True)
        df['group'] = 'group1'

    folder = Path(output)
    folder.mkdir(exist_ok=True)

    pbar = tqdm(total=df.shape[0])

    for name, group in df.groupby('group'):

        if has_group:
            outfolder = folder / Path(name)
            outfolder.mkdir(exist_ok=True)
        else:
            outfolder = folder

        if trim:
            # first file
            r1_split = group['filename'].iloc[0].split('_')
            r1_split2 = r1_split[-1].split('.')
            newtime = group['dateFromQuery'].iloc[0].strftime(format='%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
            if len(r1_split2) > 2: # bitrate is also in the name
                bitrate = r1_split2[1].split('-')[-1]
                newname = f"{'_'.join(r1_split[:-1])}_{newtime}-{bitrate}.{r1_split2[-1]}"
            else:
                newname = f"{'_'.join(r1_split[:-1])}_{newtime}.{r1_split2[-1]}"

            output_file = outfolder / Path(newname)
            if not output_file.exists():
                r1 = pd.to_datetime(r1_split2[0], format='%Y%m%dT%H%M%S', utc=True)
                r01 = group['dateFromQuery'].iloc[0]
                ss = str((r01 - r1).total_seconds())

                tmpfile = tempfile.gettempdir() / Path(newname)
                download_file(group['urlfile'].iloc[0], tmpfile)

                ff_cmd = ['ffmpeg', '-v', 'quiet','-ss', ss,
                        '-i', tmpfile,
                        '-c', 'copy', str(output_file)]
                sp.run(ff_cmd, check=False)
                tmpfile.unlink()

            pbar.update()


            # last file
            output_file = outfolder / Path(group['filename'].iloc[-1])
            if not output_file.exists():
                r2 = group['filename'].iloc[-1].split('_')[-1].split('.')[0]
                r2 = pd.to_datetime(r2, format='%Y%m%dT%H%M%S', utc=True)
                r02 = group['dateToQuery'].iloc[-1]
                to = str((r02 - r2).total_seconds())

                tmpfile = tempfile.gettempdir() / Path(output_file.name)
                download_file(group['urlfile'].iloc[-1], tmpfile)

                ff_cmd = ['ffmpeg', '-v', 'quiet', '-to', to,
                        '-i', tmpfile,
                        '-c', 'copy', str(output_file)]
                sp.run(ff_cmd, check=False)
                tmpfile.unlink()

            pbar.update()

            group = group.iloc[1:-1]


        for _, row in group.iterrows():

            output_file = outfolder / Path(row['filename'])

            if not output_file.exists():
                download_file(row['urlfile'], output_file)

            pbar.update()

    pbar.close()
