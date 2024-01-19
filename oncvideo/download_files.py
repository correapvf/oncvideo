import tempfile
from pathlib import Path
from tqdm import tqdm
from ._utils import download_file, parse_file_path, run_ffmpeg, trim_group, name_to_timestamp

def download_files(arg_input, output='output', trim=False):

    fileOut = Path(output + '.csv')
    if arg_input == fileOut.name:
        raise ValueError("Output folder must be a different name than input csv.")

    df, has_group, need_download = parse_file_path(arg_input)

    if not need_download:
        raise ValueError("Input must be a DataFrame or .csv with files to download.")

    header = 'filename,video_filename,timestamp\n'
    if has_group:
        header = 'subfolder,' + header
        df.sort_values(['group','filename'], inplace=True)
    else:
        df.sort_values(['filename'], inplace=True)
        df['group'] = 'group1'

    folder = Path(output)
    folder.mkdir(exist_ok=True)

    # check if command has been started alread
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

        for _, row in group.iterrows():

            output_file = outfolder / Path(row['filename'])
            timestamp = name_to_timestamp(row['filename'])
            timestamp = timestamp.strftime(format='%Y-%m-%d %H:%M:%S.%f')[:-3]

            if not output_file.exists():
                if row['skip'] == '': # download files not trimmed
                    success = download_file(row['urlfile'], output_file)
                    if success:
                        f.write(f"{subfolder}{row['filename']},{row['video_filename']},{timestamp}\n")
                
                else: # need to be trimmed

                    tmpfile = tempfile.gettempdir() / Path(output_file.name)
                    success = download_file(group['urlfile'].iloc[-1], tmpfile)
                    if success:
                        
                        ff_cmd = ['ffmpeg'] + row['skip'] + ['-i', tmpfile,
                            '-c', 'copy', output_file]
                        run_ffmpeg(ff_cmd, filename=output_file.name)
                        f.write(f"{subfolder}{row['filename']},{row['video_filename']},{timestamp}\n")
            
            pbar.update()

    pbar.close()
    f.close()
