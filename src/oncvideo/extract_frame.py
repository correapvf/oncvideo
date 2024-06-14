"""Function to extract frames from videos"""

from pathlib import Path
import tempfile
from tqdm import tqdm
import numpy as np
import pandas as pd
import cv2
from ._utils import download_file, to_timedelta, strftd2, parse_file_path, run_ffmpeg, LOGO
from .utils import name_to_timestamp
from ._iterate_ffmpeg import iterate_ffmpeg, iterate_init

def _ffmpeg_run_frame(input_file, output_file, skip, params, f, subfolder, video_name):
    """
    Create ffmpeg command and run and 
    write in a csv file for the generated frames
    """
    # rename output to extract frames
    file_name = output_file.stem
    outfolder = output_file.parent
    output_file = f"{outfolder / file_name}_%05d.jpg"

    # Create ffmpeg command and run
    ff_cmd = ['ffmpeg'] + skip + ['-i', input_file] + params['ffmpeg'] + [output_file]
    run_ffmpeg(ff_cmd, filename=file_name)

    # rename frames to correct timestamp
    d = outfolder.glob(file_name + "_*.jpg")
    dout = pd.DataFrame({'filename_old': list(d), 'original_video': video_name})
    if len(dout) > 0: # didn't extracted any frame (e.g. file length is lower than interval)
        dout['filename_split'] = dout['filename_old'].apply(lambda x: x.stem.split('_'))
        dout['timestamp'] = dout['filename_split'].str[-2]

        if dout['timestamp'].str.contains('Z-', regex=False).any():
            dout['timestamp'] = dout['timestamp'].str.split('-').str[0]

        dout['timestamp'] = pd.to_datetime(dout['timestamp'], format='%Y%m%dT%H%M%S.%fZ', utc=True)
        dout['timeadd'] = (dout['filename_split'].str[-1].astype(float) *
            params['interval'] - params['interval2'])
        dout['timestamp'] = dout['timestamp'] + pd.to_timedelta(dout['timeadd'], unit='sec')
        dout['filename'] = (dout['filename_split'].str[:-2].str.join('_') + '_' +
            dout['timestamp'].dt.strftime('%Y%m%dT%H%M%S.%f').str[:-3] + 'Z.jpg')

        for _, row2 in dout.iterrows():
            row2['filename_old'].rename(row2['filename_old'].with_name(row2['filename']))

        # save csv with all frames names + original videos
        if subfolder != '':
            dout['subfolder'] = subfolder[:-1]
            dout = dout[['subfolder', 'filename', 'original_video', 'timestamp']]
        else:
            dout = dout[['filename', 'original_video', 'timestamp']]

        dout.to_csv(f, mode='a', index=False, header=False, lineterminator='\n')
    else:
        with open("log_download.txt", 'a', encoding="utf-8") as ferr:
            ferr.write(f"No frame was extracted from: {input_file.name}\n")


def extract_frame(source, interval, output='frames', trim=False,
    deinterlace=False, rounding_near=False):
    """
    Extract frames at a given interval

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *)
    interval : float
        Interval, in seconds, to extract frames (e.g. a value of 10 will
        extract one frame every 10 seconds).
    output : str, default 'output'
        Name of the output folder to save converted videos
    trim : bool, default False
        Trim video files to match the initial seach query
    deinterlace : bool, default False
        Deinterlace video before getting the frames.
    rounding_near : bool, default False
        Grab frames at the middle of each interval (default ffmpeg
        behaviour). If False, will grab frames at the start of each interval.
    """
    header = 'filename,original_video,timestamp\n'

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

    params = {'ffmpeg': ['-vf', vf_cmd,
            '-qmin', '1', '-qmax', '1', '-q:v', '1'],
        'interval': interval,
        'interval2': interval2
    }

    iterate_ffmpeg(source, output, header, trim, _ffmpeg_run_frame, params)


def extract_fov(source, timestamps=None, duration=None, output='fovs', deinterlace=False):
    """
    Extract FOVs from videos

    Extract framegrabs or clips for each video at given timestamps 

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *)
    timestamps : str, float, or list
        A timestamp or a list of timestaps where the framegrabs will be extracted.
        Can be a float indicating the number of seconds or a string in the format
        mm:ss.f, from the start of the video. If None (the default), it will try to
        get the timestamps from the column 'fovs' in the source.
    duration : float
        If provided, the function will get clips instead of framegrabs, with
        duration given in seconds. The start of the clips are given by 'timestamps'. 
    output : str, default 'fovs'
        Name of the output folder to save images/videos.
    deinterlace : bool, default False
        Deinterlace video before getting the frames. This argument is ignored
        for clips, since the stream is copyied from the original video.
    """
    df, has_group, need_download = parse_file_path(source)

    fov_csv = 'fovs' in df

    if fov_csv:
        df['fovs'] = df['fovs'].str.split(',')
        df['fovs'].apply(lambda x: [to_timedelta(y) for y in x])

        df['subfolder'] = df['fovs'].apply(lambda x: [f'FOV{y+1}' for y in list(range(len(x)))])
        maxfov = df['fovs'].apply(len).max()
        fovfolder = [f'FOV{y+1}' for y in list(range(maxfov))]

    else:
        if timestamps is None:
            raise ValueError("You must provide a timestamp to extract the FOV,",
            "or the csv file must contain the column 'fovs'")
        if not isinstance(timestamps, list):
            timestamps = [timestamps]
        fovs = [to_timedelta(x) for x in timestamps]
        df['fovs'] = [fovs for _ in range(len(df))]
        fovfolder = [f'FOV_{strftd2(x)}' for x in fovs]
        df['subfolder'] = [fovfolder for _ in range(len(df))]

    # header for the output csv

    header = f"original_video,{','.join(fovfolder)}\n"

    df, folder, f = iterate_init(output, header, df, has_group)

    if deinterlace:
        vf_cmd = ['-vf', 'pp=ci|a']
    else:
        vf_cmd = []

    if duration == 'None':
        duration = None

    # start for loop
    pbar = tqdm(total=df.shape[0], desc = 'Processed files')

    try:
        for name, group in df.groupby('group'):

            if has_group:
                outfolder = folder / Path(name)
                outfolder.mkdir(exist_ok=True)
            else:
                outfolder = folder

            # Create folder for each FOV
            if fov_csv:
                maxfov = group['fovs'].apply(len).max()
                fovs = [f'FOV{y+1}' for y in list(range(maxfov))]

                for fov in fovs:
                    p = outfolder / Path(fov)
                    p.mkdir(exist_ok=True)
            else:
                for fov in fovfolder:
                    p = outfolder / Path(fov)
                    p.mkdir(exist_ok=True)

            # extract frames for each video
            for _, row in group.iterrows():
                # download video
                file_name_p = Path(row['filename'])
                if need_download:
                    tmpfile = tempfile.gettempdir() / file_name_p
                    download_file(row['urlfile'], tmpfile)
                    tmpfile_str = str(tmpfile)
                else:
                    tmpfile_str = row['urlfile']

                # get timestamp of file
                timestamp = name_to_timestamp(file_name_p.name)
                oldname_dc = timestamp.dc

                # Extract frame/video for each FOV
                filename_fovs = []
                for fov, p in zip(row['fovs'], row['subfolder']):

                    if fov == '':
                        filename_fovs.append(new_name.name)
                        continue

                    newtime = (timestamp + fov).strftime('%Y%m%dT%H%M%S.%f')[:-3]
                    filename = f"{oldname_dc}_{newtime}Z{file_name_p.suffix}"
                    new_name_p = Path(filename)
                    fov_str = str(fov.total_seconds())

                    if duration is None:
                        new_name = outfolder / p / new_name_p.with_suffix('.jpg')
                        ff_cmd = ['ffmpeg', '-ss', fov_str, '-i',
                            tmpfile_str] + vf_cmd + ['-frames:v', '1', '-update', '1',
                            '-qmin', '1', '-qmax', '1', '-q:v', '1', new_name]

                    else:
                        new_name = outfolder / p / new_name_p
                        ff_cmd =['ffmpeg', '-ss', fov_str,
                            '-i', tmpfile_str, '-t', duration, '-c', 'copy', new_name]

                    run_ffmpeg(ff_cmd, filename=new_name.name)
                    filename_fovs.append(new_name.name)

                if need_download:
                    tmpfile.unlink()

                # save csv with video name
                if has_group:
                    f.write(f"{name},{row['filename']},{','.join(filename_fovs)}\n")
                else:
                    f.write(f"{row['filename']},{','.join(filename_fovs)}\n")

                pbar.update()

    finally:
        pbar.close()
        f.close()



def make_timelapse(folder='fovs', time_format='%Y/%m/%d %Hh', fps=10, fontScale=1, logo=False, caption=None):
    """
    Generate timelapse video from images

    Parameters
    ----------
    folder : str, default 'fovs'
        Path to a folder where .jpg images are stored.
    time_format : str, default '%Y/%m/%d %Hh'
        Format how the timestamp will be writen on the video.
    fps : float, default 10
        Timelapse video FPS.
    fontScale : float, default 1
        Font scale for the timestamp. Increase values for a larger font size.
    logo : bool, default False
        Include ONC logo on the video?
    caption : str, default None
        Insert a caption at the bottom of the screen. You can break lines with <br> tag.
    """
    folder = Path(folder)

    fu = [f for f in folder.iterdir() if f.is_dir()]
    fu += [folder]

    if logo:
        logoimg = cv2.imdecode(np.frombuffer(LOGO, np.uint8), cv2.IMREAD_COLOR)

    for f in tqdm(fu, desc='Processed folders'):
        images = f.glob("*.jpg")
        images = list(images)
        if len(images) < 1:
            continue

        imgfile = images[0]
        img = cv2.imread(str(imgfile), cv2.IMREAD_GRAYSCALE)
        video_dim = img.shape[::-1]
        output_video = f.name + '.mp4'
        vidwriter = cv2.VideoWriter(output_video, cv2.VideoWriter_fourcc(*"mp4v"), fps, video_dim)

        spacing = img.shape[0] // 40 # 2.5% of the image size
        ctxt = (spacing, spacing+int(22*fontScale))
        font = cv2.FONT_HERSHEY_SIMPLEX

        if logo:
            size_logo = img.shape[0] // 13 # 7.5% of the image size
            logo_resize = cv2.resize(logoimg, (size_logo,size_logo), interpolation=cv2.INTER_LINEAR)

            # top right corner
            top_y = spacing
            left_x = img.shape[1] - spacing - size_logo
            bottom_y = spacing + size_logo 
            right_x = img.shape[1] - spacing


        for imgfile in tqdm(images, leave=False):

            img = cv2.imread(str(imgfile), cv2.IMREAD_COLOR)
            timestamp = name_to_timestamp(imgfile.name)
            timestamp = timestamp.strftime(time_format)

            # Using cv2.putText() method
            img = cv2.putText(img, timestamp, org=ctxt, fontFace=font,
                            fontScale=fontScale, color=(255, 255, 255), thickness=2, lineType=cv2.LINE_AA)

            if caption is not None:
                textY = img.shape[0]-spacing
                for line in reversed(caption.split('<br>')):
                    textsize = cv2.getTextSize(line, font, fontScale, thickness=2)[0]
                    textX = (img.shape[1] - textsize[0]) // 2
                    img = cv2.putText(img, line, org=(textX, textY), fontFace=font,
                                fontScale=fontScale, color=(255, 255, 255), thickness=2, lineType=cv2.LINE_AA)
                    textY = textY - textsize[1] - spacing

            # insert logo
            if logo:
                destination = img[top_y:bottom_y, left_x:right_x] 
                result = cv2.addWeighted(destination, 1, logo_resize, 0.5, 0)
                img[top_y:bottom_y, left_x:right_x] = result

            vidwriter.write(img)

        vidwriter.release()
