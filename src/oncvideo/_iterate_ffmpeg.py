"""Functions to execute ffmpeg in a loop"""
import tempfile
from pathlib import Path
import pandas as pd
from tqdm.auto import tqdm
from ._utils import download_file, trim_group, parse_file_path


def iterate_init(output, header, df, has_group):
    """
    Helper function for iterate_ffmpeg
    """
    if has_group:
        header = 'subfolder,' + header
        df.sort_values(['group','filename'], inplace=True)
    else:
        df.sort_values(['filename'], inplace=True)
        df['group'] = 'group1'

    folder = Path(output)
    folder.mkdir(exist_ok=True)

    # check if command has been started alread
    output_pathlib = Path(output)
    file_out = output_pathlib / (output_pathlib.name + '.csv')
    if file_out.exists():
        tmp = pd.read_csv(file_out)
        count = df.shape[0]
        df = df[~df['filename'].isin(tmp['original_video'])]
        count -= df.shape[0]
        f = open(file_out, "a", encoding="utf-8")
        print(f"{file_out.name} already exists! {count} files already processed, "
            "skipping to remaining files.")
    else:
        f = open(file_out, "w", encoding="utf-8")
        f.write(header)

    return df, folder, f


def iterate_ffmpeg(source, output, header, trim, ffmpeg_run, params, missing_ok=False):
    """
    Loop in the DataFrame and execute the ffmpeg command
    """
    df, has_group, need_download = parse_file_path(source)

    df, folder, f = iterate_init(output, header, df, has_group)

    pbar = tqdm(total=df.shape[0], desc = 'Processed files')

    try:
        for name, group in df.groupby('group'):

            if has_group:
                outfolder = folder / Path(name)
                outfolder.mkdir(exist_ok=True)
                subfolder = name + ','
            else:
                outfolder = folder
                subfolder = ''

            group = group.copy()
            group['original_video'] =  group['filename'].copy()
            group['skip'] = [[]] * len(group)

            if trim:
                group = trim_group(group)

            # convert each file in the group
            for _, row in group.iterrows():

                output_file = outfolder / Path(row['filename'])

                if need_download:
                    tmpfile = tempfile.gettempdir() / Path(row['filename'])
                    success = download_file(row['urlfile'], tmpfile)
                    if not success:
                        continue
                else:
                    tmpfile = row['urlfile']

                ffmpeg_run(tmpfile, output_file, row['skip'], params, f,
                    subfolder, row['original_video'])

                if need_download:
                    tmpfile.unlink(missing_ok)

                pbar.update()

    finally:
        pbar.close()
        f.close()
