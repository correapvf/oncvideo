"""Get parameters from videos"""
import json
from pathlib import Path
import subprocess as sp
import pandas as pd
from tqdm.auto import tqdm
from ._utils import strftd, parse_file_path

def _meta_video(urlfile, check_interlaced):
    """
    Create ffprobe command and run to get
    parameters from videos
    """
    ffprobe_cmd = ['ffprobe', '-v', 'quiet',
                    '-select_streams', 'v:0',
                    '-show_entries', ('stream=codec_name,pix_fmt,bit_rate,'
                        'display_aspect_ratio,width,height,r_frame_rate,'
                        'avg_frame_rate,field_order:format=size,duration'),
                    '-of', 'json',
                    '-i', urlfile]

    out_raw = sp.check_output(ffprobe_cmd)
    out_dict = json.loads(out_raw)

    stream = out_dict['streams'][0]

    # define variables
    codec = stream['codec_name'] if "codec_name" in stream else 'unknown'
    pix_fmt = stream['pix_fmt'] if "pix_fmt" in stream else 'unknown'
    bit_rate = float(stream['bit_rate']) / 1000  if "bit_rate" in stream else 'unknown'
    aspect = stream['display_aspect_ratio'] if "display_aspect_ratio" in stream else 'N/A'
    frame_w = stream['width'] if "width" in stream else 'unknown'
    frame_h = stream['height'] if "height" in stream else 'unknown'
    fps_set = stream['r_frame_rate'] if "r_frame_rate" in stream else 'unknown'
    fps_avg = stream['avg_frame_rate'] if "avg_frame_rate" in stream else 'unknown'
    scan_type = stream['field_order'] if "field_order" in stream else 'unknown'
    file_size = int(out_dict['format']['size']) * 9.5367431640625e-07
    nseconds = float(out_dict['format']['duration'])

    # check if video is interlaced or not
    if check_interlaced:
        ffprobe_cmd = ['ffmpeg', '-ss', '00:00:01'
                    '-i', urlfile,
                    '-frames:v', '30',
                    '-vf', 'idet,metadata=print:file=-',
                    '-f', 'null', '-']

        out_raw = sp.check_output(ffprobe_cmd)
        out_str = out_raw.decode('utf-8')
        lines = out_str.split('\n')
        tally = [x.split('=')[1] for x in lines if "lavfi.idet.multiple.current_frame" in x]
        scan_type = pd.Series(tally).mode()[0]

    fps = fps_avg.split('/')
    fps = float(fps[0]) / float(fps[1])
    fps_mode = 'Constant' if fps_avg==fps_set else 'Variable'

    duration = strftd(nseconds)

    return (f'{codec},{pix_fmt},{bit_rate},{fps},{fps_mode},{scan_type},'
        f'{aspect},{frame_w},{frame_h},{duration},{file_size}')

def video_info(source, output='video_info.csv', check_interlaced=False):
    """
    Get video information

    Include video codec, pixel format, bitrate, fps, fps mode (constant or variable), scan type (
    progressive or interlaced), aspect ratio, width, height, duration and file size.
    Note that scan type is based in the video metadata, unless check_interlaced is set to True,
    where idet filter is used to check scan type. Video duration is based from the file, and may
    differ from the duration returned by 'list' or 'blist', which is based on the API call.

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to
        match multiple files (use *)
    output : str, default 'video_info.csv'
        Name of the csv file to save video information
    check_interlaced : bool, default False
        Use the idet filter from ffmpeg to check if video is interlaced.
    """
    df, has_group, _ = parse_file_path(source)

    # configure csv
    header = ('filename,codec,pix_fmt,bit_rate_kbps,fps,fps_mode,scan_type,'
        'aspect_ratio,width,height,duration,fileSize_MB\n')
    seps = ',,,,,,,,,,'
    if has_group:
        header = 'group,' + header
        seps = ',' + seps

    # check if we have to create a new file or continue an existing job
    file_out = Path(output)
    if file_out.exists():
        with open(file_out, encoding="utf-8") as f:
            count = sum(1 for _ in f)
        count -= 1
        df = df.loc[count:]
        f = open(file_out, "a", encoding="utf-8")
        print(f"{output} already exists! {count} files already processed,",
            "skipping to remaining files.")
    else:
        f = open(file_out, "w", encoding="utf-8")
        f.write(header)


    for _, row in tqdm(df.iterrows(), total=df.shape[0]):

        to_write = f"{row['group']},{row['filename']}" if has_group else row['filename']

        try:
            info = _meta_video(row['urlfile'], check_interlaced)
            f.write(f'{to_write},{info}\n')
        except RuntimeError:
            f.write(f'{to_write}{seps}\n')

    f.close()
