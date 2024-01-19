import tempfile
from pathlib import Path
from tqdm import tqdm
from ._utils import download_file, run_ffmpeg, name_to_timestamp, trim_group, parse_file_path

def to_mp4(arg_input, output='output', trim=False, deinterlace=False, crf=None):
    """
    Add up two integer numbers.

    This function simply wraps the ``+`` operator, and does not
    do anything interesting, except for illustrating what
    the docstring of a very simple function looks like.

    Parameters
    ----------
    arg_input : str or DataFrame
        A pandas DataFrame, or a str to .csv file
    num2 : int
        Second number to add.

    Returns
    -------
    int
        The sum of ``num1`` and ``num2``.
    """

    fileOut = Path(output + '.csv')
    if arg_input == fileOut.name:
        raise ValueError("Output folder must be a different name than input csv.")

    df, has_group, need_download = parse_file_path(arg_input)

    header = 'filename,video_filename,timestamp\n'
    if has_group:
        header = 'subfolder,' + header
        df.sort_values(['group','filename'], inplace=True)
    else:
        df.sort_values(['filename'], inplace=True)
        df['group'] = 'group1'

    folder = Path(output)
    folder.mkdir(exist_ok=True)

    # video filter
    vf_cmd = 'fps=source_fps' # force constant frame rate
    if deinterlace:
        vf_cmd = 'pp=ci|a,' + vf_cmd

    if crf is None:
        crf = []
    else:
        crf = ['-crf', str(crf)]

    # check if command has been started already
    if fileOut.exists():
        f = open(fileOut, "a", encoding="utf-8")
    else:
        f = open(fileOut, "w", encoding="utf-8")
        f.write(header)

    pbar = tqdm(total=df.shape[0], desc = 'Processed files')

    for name, group in df.groupby('group'):

        if has_group:
            outfolder = folder / Path(name)
            outfolder.mkdir(exist_ok=True)
            subfolder = name + ','
        else:
            outfolder = folder
            subfolder = ''

        group = group.copy()
        group['video_filename'] =  group['filename'].copy()
        group['skip'] = [[]] * len(group)

        if trim:
            group = trim_group(group)
        
        # convert each file
        for _, row in group.iterrows():

            output_file = outfolder / Path(row['filename'])
            output_file = output_file.with_suffix('.mp4')
            timestamp = name_to_timestamp(row['filename']).strftime(format='%Y-%m-%d %H:%M:%S.%f')[:-3]

            if not output_file.exists():
                if need_download:
                    tmpfile = tempfile.gettempdir() / Path(row['filename'])
                    success = download_file(row['urlfile'], tmpfile)
                    if not success:
                        continue
                else:
                    tmpfile = row['urlfile']

                ff_cmd = ['ffmpeg'] + row['skip'] + ['-i', tmpfile,
                    '-vf', vf_cmd, '-c:v', 'libx264'] + crf + ['-an',
                    '-movflags', '+faststart', output_file]

                run_ffmpeg(ff_cmd, filename=output_file.name)

                tmpfile.unlink()
                f.write(f"{subfolder}{output_file.name},{row['video_filename']},{timestamp}\n")

            pbar.update()

    pbar.close()
    f.close()
   