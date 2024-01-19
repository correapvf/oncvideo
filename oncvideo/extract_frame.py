from pathlib import Path
import tempfile
import pandas as pd
from tqdm import tqdm 
from ._utils import parse_file_path, download_file, run_ffmpeg, trim_group


def extractFrame(arg_input, interval=1, output='frames', trim=False, deinterlace=False, rounding_near=False):

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

    if interval < 1.0:
        vf_cmd = f'fps={1/interval}'
    elif interval == 1.0:
        vf_cmd = 'fps=1'
    else:
        vf_cmd = f'fps=1/{interval}'
    
    if deinterlace:
        vf_cmd = 'pp=ci|a,' + vf_cmd
    
    if rounding_near:
        interval2 = interval / 2
    else:
        vf_cmd = vf_cmd + ':round=up'
        interval2 = interval


    # check if command has been started alread
    if fileOut.exists():
        tmp = pd.read_csv(fileOut)
        count = df.shape[0]
        df = df[~df['filename'].isin(tmp['video_filename'])]
        count -= df.shape[0]
        f = open(fileOut, "a", newline='', encoding="utf-8")
        print(f"{fileOut.name} already exists! {count} files already processed, skipping to remaining files.")
    else:
        f = open(fileOut, "w", newline='', encoding="utf-8")
        f.write(header)

    pbar = tqdm(total=df.shape[0], desc = 'Processed files')
    
    for name, group in df.groupby('group'):
        
        if has_group:
            outfolder = folder / Path(name)
            outfolder.mkdir(exist_ok=True)
        else:
            outfolder = folder
        
        group = group.copy()
        group['video_filename'] =  group['filename'].copy()
        group['skip'] = [[]] * len(group)

        if trim:
            group = trim_group(group)

        # start interating for each file
        for _, row in group.iterrows():
            # extract frames for each video
            file_name = Path(row['filename']).stem
            if need_download:
                tmpfile = tempfile.gettempdir() / Path(row['filename'])
                success = download_file(row['urlfile'], tmpfile)
                if not success:
                    continue
                tmpfile_str = str(tmpfile)
            else:
                tmpfile_str = row['urlfile']
            
            ff_cmd = ['ffmpeg', '-v', 'quiet'] + row['skip']
            ff_cmd += ['-i', tmpfile_str,
                '-vf', vf_cmd,
                '-qmin', '1', '-qmax', '1', '-q:v', '1',
                f"{outfolder / file_name}_%05d.jpg"]
            

            run_ffmpeg(ff_cmd, file_name)

            if need_download:
                tmpfile.unlink()


            # rename frames to correct timestamp        
            d = outfolder.glob(file_name + "_*.jpg")
            dout = pd.DataFrame({'filename_old': list(d), 'video_filename': row['video_filename']})
            if len(dout) > 0: # didn't extracted any frame (e.g. file length is lower than interval)
                dout['filename_split'] = dout['filename_old'].apply(lambda x: x.stem.split('_'))
                dout['timestamp'] = dout['filename_split'].str[-2]

                if dout['timestamp'].str.contains('Z-', regex=False).any():
                    dout['timestamp'] = dout['timestamp'].str.split('-').str[0]

                dout['timestamp'] = pd.to_datetime(dout['timestamp'], format='%Y%m%dT%H%M%S.%fZ', utc=True)
                dout['timeadd'] = dout['filename_split'].str[-1].astype(float) * interval - interval2
                dout['timestamp'] = dout['timestamp'] + pd.to_timedelta(dout['timeadd'], unit='sec')
                dout['filename'] = dout['filename_split'].str[:-2].str.join('_') + '_' + dout['timestamp'].dt.strftime('%Y%m%dT%H%M%S.%f').str[:-3] + 'Z.jpg'

                for _, row2 in dout.iterrows():
                    row2['filename_old'].rename(row2['filename_old'].with_name(row2['filename']))

                # save csv with all frames names + original videos
                if has_group:
                    dout['subfolder'] = name
                    dout = dout[['subfolder', 'filename', 'video_filename', 'timestamp']]
                else:
                    dout = dout[['filename', 'video_filename', 'timestamp']]
                
                dout.to_csv(f, mode='a', index=False, header=False)
            else:
                with open("log_download.txt", 'a', encoding="utf-8") as ferr:
                    ferr.write(f"No frame was extracted from: {row['urlfile']}\n")
            
            pbar.update()

    pbar.close()
    f.close()
