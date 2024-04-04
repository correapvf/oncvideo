"""Module providing functions to download and convert video."""
import shutil
from ._utils import run_ffmpeg
from .utils import name_to_timestamp
from ._iterate_ffmpeg import iterate_ffmpeg


def _ffmpeg_run_download(input_file, output_file, skip, params, f, subfolder, video_name):
    """
    Create and run ffmpeg command and save csv file
    """
    # get timestamp from filename
    timestamp = name_to_timestamp(output_file.name)
    timestamp = timestamp.strftime(format='%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Create ffmpeg command and run
    if len(skip) == 0: # no need to crop video, just move file
        shutil.move(input_file, output_file)
    else:
        ff_cmd = ['ffmpeg'] + skip + ['-i', input_file] + params + [output_file]
        run_ffmpeg(ff_cmd, filename=output_file.name)

    # write in csv file
    f.write(f"{subfolder}{output_file.name},{video_name},{timestamp}\n")


def download_files(source, output='output', trim=False):
    """
    Download files from the table provided by source

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, or a path to .csv file
    output : str, default 'output'
        Name of the output folder to save converted videos
    trim : bool, default False
        Trim video files to match the initial seach query
    """
    if '*' in source:
        raise ValueError("Input must be a DataFrame or .csv with files to download.")

    header = 'filename,original_video,timestamp\n'

    params = ['-c', 'copy']

    iterate_ffmpeg(source, output, header, trim, _ffmpeg_run_download, params, True)


def _ffmpeg_run_mp4(input_file, output_file, skip, params, f, subfolder, video_name):
    """
    Create and run ffmpeg command and save csv file
    """
    # get timestamp from filename
    timestamp = name_to_timestamp(output_file.name)
    timestamp = timestamp.strftime(format='%Y-%m-%d %H:%M:%S.%f')[:-3]

    # change extension
    output_file = output_file.with_suffix('.mp4')

    # Create ffmpeg command and run
    ff_cmd = ['ffmpeg'] + skip + ['-i', input_file] + params + [output_file]
    run_ffmpeg(ff_cmd, filename=output_file.name)

    # write in csv file
    f.write(f"{subfolder}{output_file.name},{video_name},{timestamp}\n")


def to_mp4(source, output='output', trim=False, deinterlace=False, crf=None,
    keep_audio=False, yuv420=False, h265=False):
    """
    Convert video to mp4

    Videos are encoded to mp4 (h264) constrained by quality
    (Constant Rate Factor), with a faststart option for web video,
    and constant framerate.

    Parameters
    ----------
    source : str or pandas.DataFrame
        A pandas DataFrame, a path to .csv file, or a Glob pattern to 
        match multiple files (use *)
    output : str, default 'output'
        Name of the output folder to save converted videos
    trim : bool, default False
        Trim video files to match the initial seach query
    deinterlace : bool, default False
        Deinterlace video.
    crf : int, defalt None
        Set CRF (quality level) in ffmpeg. The default will use the
        default value from ffmpeg.
    keep_audio : bool, default False
        Keep audio in the video? Default is to remove de audio.
    yuv420 : bool, defalt False
        Force YUV planar color space with 4:2:0 chroma subsampling.
        May be needed to display video in some players/browsers.
    h265 : bool, defalt False
        Use H.265 encoding instead of H.264. H.265 offers a higher
        compression, but may not be supported by some players/browsers.
    """

    header = 'filename,original_video,timestamp\n'

    # video filter
    vf_cmd = 'fps=source_fps' # force constant frame rate

    if deinterlace:
        vf_cmd = 'pp=ci|a,' + vf_cmd

    if yuv420:
        vf_cmd = vf_cmd + ',format=yuv420p'

    crfv = [] if crf is None else ['-crf', str(crf)]

    encoder = 'libx265' if h265 else 'libx264'

    audio = ['-c:a', 'aac', '-b:a', '128k'] if keep_audio else ['-an', '-sn']

    params = ['-vf', vf_cmd, '-c:v', encoder,
        '-preset', 'slow'] + crfv + audio + ['-movflags', '+faststart']

    # run loop
    iterate_ffmpeg(source, output, header, trim, _ffmpeg_run_mp4, params)
