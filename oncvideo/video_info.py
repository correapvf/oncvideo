# DOWNLOAD VIDEO FILES
import json
from pathlib import Path
import pandas as pd
import subprocess as sp
from tqdm import tqdm
from ._utils import strftd, parse_file_path

def meta_video(urlfile, check_interlaced):
    ffprobe_cmd = ['ffprobe', '-v', 'quiet',
                    '-select_streams', 'v:0',
                    '-show_entries', 'stream=codec_name,pix_fmt,bit_rate,display_aspect_ratio,width,height,r_frame_rate,avg_frame_rate,field_order:format=size,duration',
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
    frame_W = stream['width'] if "width" in stream else 'unknown'
    frame_H = stream['height'] if "height" in stream else 'unknown'
    fps_set = stream['r_frame_rate'] if "r_frame_rate" in stream else 'unknown'
    fps_avg = stream['avg_frame_rate'] if "avg_frame_rate" in stream else 'unknown'
    scan_type = stream['field_order'] if "field_order" in stream else 'unknown'
    fileSize = int(out_dict['format']['size']) * 9.5367431640625e-07
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

    return f'{codec},{pix_fmt},{bit_rate},{fps},{fps_mode},{scan_type},{aspect},{frame_W},{frame_H},{duration},{fileSize}'

def video_info(arg_input, output='video_info.csv', check_interlaced=False):
    df, has_group, _ = parse_file_path(arg_input)

    # configure csv
    header = 'filename,codec,pix_fmt,bit_rate_kbps,fps,fps_mode,scan_type,aspect_ratio,width,height,duration,fileSize_MB\n'
    seps = ',,,,,,,,,,'
    if has_group:
        header = 'group,' + header
        seps = ',' + seps

    # check if we have to create a new file or continue an existing job
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
            info = meta_video(row['urlfile'], check_interlaced)
            f.write(f'{to_write},{info}\n')
        except RuntimeError:
            f.write(f'{to_write}{seps}\n')

    f.close()

