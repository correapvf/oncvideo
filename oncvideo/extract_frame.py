from pathlib import Path
import tempfile
import subprocess as sp
import pandas as pd
from tqdm import tqdm 
from ._utils import parse_file_path, parse_trim, download_file


def extractFrame(arg_input, interval=1, output='frames', trim=False, deinterlace=False, rounding_near=False):

    df, has_group, need_download = parse_file_path(arg_input)

    if trim:
        df = parse_trim(arg_input, df)

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
    fileOut = Path(output + '.csv')
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

    pbar = tqdm(total=df.shape[0])
    
    for name, group in df.groupby('group'):
        
        if has_group:
            outfolder = folder / Path(name)
            outfolder.mkdir(exist_ok=True)
        else:
            outfolder = folder
        
        group = group.copy()
        group['video_filename'] =  group['filename'].copy()
        group['skip'] = [[] for _ in range(len(group))]
        if trim:
            # first file
            r1_split = group['filename'].iloc[0].split('_')
            r1_split2 = r1_split[-1].split('.')

            r1 = pd.to_datetime(r1_split2[0], format='%Y%m%dT%H%M%S', utc=True)
            r01 = group['dateFromQuery'].iloc[0]
            rdiff1 = (r01 - r1).total_seconds()
            if rdiff1 > 0: # Only if datefromquery is after date of file
                newtime = group['dateFromQuery'].iloc[0].strftime(format='%Y%m%dT%H%M%S.%f')[:-3] + 'Z'
                r1_split3 = r1_split2[1].split('-')
                if len(r1_split3) > 1: # bitrate is also in the name
                    bitrate = r1_split3[1]
                    newname = f"{'_'.join(r1_split[:-1])}_{newtime}-{bitrate}.{r1_split2[-1]}"
                else:
                    newname = f"{'_'.join(r1_split[:-1])}_{newtime}.{r1_split2[-1]}"
                
                group['filename'].iloc[0] = newname
                group['skip'].iloc[0] = ['-ss', str(rdiff1)]



            # last file
            r2 = group['filename'].iloc[-1].split('_')[-1].split('.')[0]
            r2 = pd.to_datetime(r2, format='%Y%m%dT%H%M%S', utc=True)
            r02 = group['dateToQuery'].iloc[-1]
            rdiff2 = (r02 - r2).total_seconds()
            group['skip'].iloc[-1] = ['-to', str(rdiff2)]

        
        for _, row in group.iterrows():
            # extract frames for each video
            file_name = Path(row['filename']).stem
            if need_download:
                tmpfile = tempfile.gettempdir() / Path(row['filename'])
                download_file(row['urlfile'], tmpfile)
                tmpfile_str = str(tmpfile)
            else:
                tmpfile_str = row['urlfile']
            
            ff_cmd = ['ffmpeg', '-v', 'quiet'] + row['skip']
            ff_cmd += ['-i', tmpfile_str,
                '-vf', vf_cmd,
                '-qmin', '1', '-qmax', '1', '-q:v', '1',
                f"{outfolder / file_name}_%05d.jpg"]
            

            sp.run(ff_cmd, check=False)
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
                with open("log_download.txt", 'a', encoding="utf-8") as f:
                    f.write(f"No frame was extracted from: {row['urlfile']}\n")
            
            pbar.update()

    pbar.close()
    f.close()
