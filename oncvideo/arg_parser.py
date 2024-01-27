"""Functions to allow to run commands in the terminal"""
import os
import argparse
from onc.onc import ONC
from .list_files import list_file, list_file_batch
from .dives_onc import get_dives
from .video_info import video_info
from .didson_file import didson_info
from .extract_frame import extract_frame, extract_fov
from .download_files import download_files, to_mp4


def onc(token = None):
    """
    Create an ONC class object
    
    Create an ONC class object, but try to use environment variables to get the API token.
    The token must stored under 'ONC_API_TOKEN'.

    Parameters
    ----------
    token : str
        ONC API token

    Returns
    -------
    onc.ONC
        ONC class object
    """
    token = token if token else os.getenv('ONC_API_TOKEN')
    if token is None:
         raise ValueError("No API credentials were provided!")
    return ONC(token)


# Default functions used by each subcommand
def flist(args):
    """
    use list_file and save output
    """
    onc_ob = onc(args.token)
    df = list_file(onc_ob, deviceCode=args.deviceCode, deviceId=args.deviceId,
                   locationCode=args.locationCode, dive=args.dive, deviceCategoryCode=args.deviceCategoryCode,
                   dateFrom=args.dateFrom, dateTo=args.dateTo, quality=args.quality, extension=args.extension,
                   statistics=args.statistics)
    df.to_csv(args.output, index=False)


def fbatch(args):
    """
    use list_file_batch and save output
    """
    onc_ob = onc(args.token)
    df = list_file_batch(onc_ob, args.csvfile, quality=args.quality, extension=args.extension,
                         statistics=args.statistics)
    df.to_csv(args.output, index=False)


def fgetdives(args):
    """
    use get_dives and save output
    """
    onc_ob = onc(args.token)
    df = get_dives(onc_ob, args.nolocation)
    df.to_csv(args.output, index=False)


def finfo(args):
    """
    use get_dives and save output
    """
    video_info(args.input, args.output, args.interlaced)


def fdidson(args):
    """
    run didson_info function
    """
    didson_info(args.input, args.output)


def fdownload(args):
    """
    run download_files function
    """
    download_files(args.input, args.output, args.trim)


def fextframe(args):
    """
    run extract_frame function
    """
    extract_frame(args.input, args.interval, args.output,
                  args.trim, args.deinterlace, args.rounding_near)


def ftomp4(args):
    """
    use get_dives and save output
    """
    to_mp4(args.input, args.output, args.trim,
           args.deinterlace, args.target_quality)


def fextfov(args):
    """
    run extract_fov function
    """
    args.timestamps = args.timestamps.split(",")
    extract_fov(args.input, args.timestamps, args.duration,
                args.output, args.deinterlace)


def main():
    """
    Create parser for arguments
    """
    help_input = "A folder containing video files, or a csv file with list of \
        archived filenames (output of 'list' command)."

    parser = argparse.ArgumentParser(
        description="Commands to list and process videos files archived in Ocean3.0.")

    subparsers = parser.add_subparsers(title="Valid commands",
                                       description="For more details on one command: oncvideo <command> -h")

    # List command
    subparser_list = subparsers.add_parser(
        'list', help="List video files for a specific camera between two dates")
    subparser_list.add_argument('-t', '--token', help='API token')
    group_list = subparser_list.add_mutually_exclusive_group(required=True)
    group_list.add_argument(
        '-dc', '--deviceCode', help='Get videos from a specific Device')
    group_list.add_argument(
        '-di', '--deviceId', help='Get videos from a specific DeviceId')
    group_list.add_argument(
        '-lc', '--locationCode', help='Get videos from a specific Location')
    group_list.add_argument('-dive', help='Get videos from a specific Dive')

    subparser_list.add_argument('-dcc', '--deviceCategoryCode', default='ask',
                                help="Only used for locationCode. Usually 'VIDEOCAM' for fixed cameras and 'ROV_CAMERA' \
        for ROVs. 'ask' will list avaiable options and ask user to choose one.")
    subparser_list.add_argument('-from',
                                '--dateFrom', help='Return videos after specified time, as yyyy-mm-ddTHH:MM:SS.sssZ')
    subparser_list.add_argument('-to',
                                '--dateTo', help='Return videos before specified time, as yyyy-mm-ddTHH:MM:SS.sssZ')
    subparser_list.add_argument('-q', '--quality', default='ask', help="Quality of the videos to use. Usually should be LOW, 1500, 5000, UHD. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_list.add_argument('-ext', '--extension', default="mp4", help="File extension to search. Deapult to 'mp4'. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_list.add_argument('-s', '--statistics', action="store_false",
                                help='Do not save video durations and file sizes.')

    subparser_list.add_argument('-o', '--output', default="videos.csv",
                                help="File name to output video filenames. Default 'videos.csv'")
    subparser_list.set_defaults(func=flist)

    # List Batch command
    subparser_blist = subparsers.add_parser(
        'blist', help="List video files based on parameters stored in a csv file.")
    subparser_blist.add_argument('-t', '--token', help='API token')
    subparser_blist.add_argument(
        'csvfile', help='Csv file with arguments to interate')

    subparser_blist.add_argument('-q', '--quality', default='ask', help="Quality of the videos to use. Usually should be LOW, 1500, 5000, UHD. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_blist.add_argument('-ext', '--extension', default="mp4", help="File extension to search. Deapult to 'mp4'. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_blist.add_argument('-s', '--statistics', action="store_false",
                                 help='Do not save video durations and file sizes.')
    subparser_blist.add_argument('-k', '--keep', action="store_true",
                                 help='Keep other columns from csvfile into the output.')
    subparser_blist.add_argument('-o', '--output', default="videos.csv",
                                 help="File name to output video filenames. Default 'videos.csv'")
    subparser_blist.set_defaults(func=fbatch)

    # get dives
    subparser_dives = subparsers.add_parser(
        'getDives', help="Create a csv file listing dives from Oceans3.0")
    subparser_dives.add_argument('-t', '--token', help='API token')
    subparser_dives.add_argument('-o', '--output', default="dives.csv",
                                 help="File name to output dives. Default 'dives.csv'")
    subparser_dives.add_argument('-l', '--nolocation', action="store_false",
                                 help='Do not get locationCodes from dives')
    subparser_dives.set_defaults(func=fgetdives)

    # getInfo
    subparser_info = subparsers.add_parser(
        'info', help="Extract video information (duration, resolution, fps)")
    subparser_info.add_argument('input', help=help_input)
    subparser_info.add_argument('-o', '--output', default="video_info.csv",
                                help="File name to write information. Default 'video_info.csv'")
    subparser_info.add_argument('-i', '--interlaced', action="store_true",
                                help='Check if video is interlaced or not using the idet filter.')
    subparser_info.set_defaults(func=finfo)

    # getDIDSON
    subparser_didson = subparsers.add_parser(
        'didson', help="Extract information of DIDSON files")
    subparser_didson.add_argument('input', help=help_input)
    subparser_didson.add_argument('-o', '--output', default="DIDSON_info.csv",
                                  help="File name to write information. Default 'DIDSON_info.csv'")
    subparser_didson.set_defaults(func=fdidson)

    # Download
    subparser_download = subparsers.add_parser(
        'download', help="Download files")
    subparser_download.add_argument(
        'input', help="A csv file with list of archived filenames (output of 'list' command).")
    subparser_download.add_argument('-o', '--output', default="output",
                                    help="Folder to download files. Default 'output'")
    subparser_download.add_argument('-t', '--trim', action="store_true",
                                    help='Trim video files to match the initial seach query.')
    subparser_download.set_defaults(func=fdownload)

    # Convert to mp4
    subparser_tomp4 = subparsers.add_parser(
        'tomp4', help="Convert video to mp4 format")
    subparser_tomp4.add_argument(
        'input', help="A csv file with list of archived filenames (output of 'list' command).")
    subparser_tomp4.add_argument('-o', '--output', default="output",
                                 help="Folder to download files. Default 'output'")
    subparser_tomp4.add_argument('-t', '--trim', action="store_true",
                                 help='Trim video files to match the initial seach query.')
    subparser_tomp4.add_argument('-d', '--deinterlace', action="store_true",
                                 help='Deinterlace video. Default to False.')
    subparser_tomp4.add_argument('-crf', '--target_quality', type=float,
                                 help='Set CRF (quality level) in ffmpeg.')
    subparser_tomp4.set_defaults(func=ftomp4)

    # extract Frame
    subparser_extframe = subparsers.add_parser(
        'extframe', help="Extract frames from video files")
    subparser_extframe.add_argument('input', help=help_input)
    subparser_extframe.add_argument('interval', type=float,
                                    help="Get frames every 'X' seconds. Default to 1 second.")
    subparser_extframe.add_argument('-o', '--output', default="frames",
                                    help="Folder to download frames. Default 'frames'")
    subparser_extframe.add_argument('-t', '--trim', action="store_true",
                                    help='Trim video files to match the initial seach query.')
    subparser_extframe.add_argument('-d', '--deinterlace', action="store_true",
                                    help='Deinterlace video. Default to False.')
    subparser_extframe.add_argument('-n', '--rounding_near', action="store_true",
                                    help='Use ffmpeg deafult Timestamp rounding method (near) for fps filter.\
                                Default to False (stills start in the beginning of the video).')
    subparser_extframe.set_defaults(func=fextframe)

    # extract FOV
    subparser_extframe = subparsers.add_parser(
        'extfov', help="Extract FOVs (frames or videos) from video files")
    subparser_extframe.add_argument('input', help=help_input)
    subparser_extframe.add_argument('timestamps',
                                    help="Get frames at the specific timestamps, in seconds or mm:ss.f format.\
                                Can be a comma separated list to extract multiple FOVs.\
                                If 'durations' is suplied, will extract videos starting at each timestamp.")
    subparser_extframe.add_argument('-t', '--duration', required=False,
                                    help="Duration of the video to download. in seconds or mm:ss.f format.\
                                Can be a single value or a comma separeted list, same size as 'timestamps'.")
    subparser_extframe.add_argument('-o', '--output', default="fovs",
                                    help="Folder to download files. Default 'fovs'")
    subparser_extframe.add_argument('-d', '--deinterlace', action="store_true",
                                    help='Deinterlace video before getting frame. Default to False.')
    subparser_extframe.set_defaults(func=fextfov)

    args = parser.parse_args()
    args.func(args)


# TO DO
# Download from seatube pro and seatubeV3 (and get a frame?)
# download NAV and CTD files from same site
# test script
if __name__ == '__main__':
    main()
