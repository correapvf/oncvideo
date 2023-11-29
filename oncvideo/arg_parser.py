from os import linesep
import argparse
from pandas import Timestamp
from onc.onc import ONC
from .list_files import list_file, list_file_batch
from .dives_ONC import getDives
from .video_info import video_info
from .DIDSON_info import didson_info
from .extract_frame import extractFrame
from .download_files import download_files
from .extract_FOV import extractFOV

# Default functions used by each subcommand
def flist(args):
    onc = ONC(args.token)
    df = list_file(onc, deviceCode=args.deviceCode, deviceId=args.deviceId,
        locationCode=args.locationCode, dive=args.dive, deviceCategoryCode=args.deviceCategoryCode,
        dateFrom=args.dateFrom, dateTo=args.dateTo, quality=args.quality, extension=args.extension,
        statistics=args.statistics)

    timenow = Timestamp.now(tz="UTC").strftime('%Y-%m-%d %H:%M:%S UTC')
    with open(args.output, 'w', newline='', encoding="utf-8") as f:
        f.write(f'# Ocean Networks Canada Data Archive{linesep}')
        f.write(f"# dateFrom: {df.QuerrydateFrom}{linesep}")
        f.write(f"# dateTo: {df.QuerrydateTo}{linesep}")
        f.write(f'# File Creation Date: {timenow}{linesep}')
        df.to_csv(f, mode='a', index=False)
    

def fbatch(args):
    onc = ONC(args.token)
    df = list_file_batch(onc, args.csvfile, quality=args.quality, extension=args.extension,
        statistics=args.statistics)
    
    timenow = Timestamp.now(tz="UTC").strftime('%Y-%m-%d %H:%M:%S UTC')
    with open(args.output, 'w', newline='', encoding="utf-8") as f:
        f.write(f'# Ocean Networks Canada Data Archive{linesep}')
        f.write(f'# Batch file: {args.csvfile}{linesep}')
        f.write(f'# File Creation Date: {timenow}{linesep}')
        df.to_csv(f, mode='a', index=False)

def fgetDives(args):
    onc = ONC(args.token)
    df = getDives(onc, args.nolocation)
    df.to_csv(args.output, index=False)

def finfo(args):
    video_info(args.input, args.output, args.interlaced)

def fdidson(args):
    didson_info(args.input, args.output)

def fdownload(args):
    download_files(args.input, args.output, args.trim)

def fextframe(args):
    extractFrame(args.input, args.interval, args.output, args.trim, args.deinterlace, args.rounding_near)

def fextFOV(args):
    # convert comma separated to list
    args.timestamps = [x for x in args.timestamps.split(",")]
    if args.duration is not None:
        args.duration = [x for x in args.duration.split(",")]
        if len(args.duration) == 1:
            args.duration = args.duration * len(args.timestamps)
    
    extractFOV(args.input, args.timestamps, args.duration, args.output, args.deinterlace)

def main():

    help_input = "A folder containing video files, or a csv file with list of \
        archived filenames (output of 'list' command)."

    parser = argparse.ArgumentParser(
        description="Commands to list and process videos files archived in Ocean3.0.")

    subparsers = parser.add_subparsers(title="Valid commands",
                                       description="For more details on one command: oncvideo <command> -h")

    # List command
    subparser_list = subparsers.add_parser(
        'list', help="List video files for a specific camera between two dates")
    subparser_list.add_argument('token', help='API token')
    group_list = subparser_list.add_mutually_exclusive_group(required=True)
    group_list.add_argument(
        '-dc','--deviceCode', help='Get videos from a specific Device')
    group_list.add_argument(
        '-di','--deviceId', help='Get videos from a specific DeviceId')
    group_list.add_argument(
        '-lc','--locationCode', help='Get videos from a specific Location')
    group_list.add_argument('-dive', help='Get videos from a specific Dive')
    
    subparser_list.add_argument('-dcc','--deviceCategoryCode', default='ask', help="Only used for locationCode. Usually 'VIDEOCAM' for fixed cameras \
        and 'ROV_CAMERA' for ROVs. 'ask' will list avaiable options and ask user to choose one.")
    subparser_list.add_argument('-from',
        '--dateFrom', help='Return videos after specified time, as yyyy-mm-ddTHH:MM:SS.sssZ')
    subparser_list.add_argument('-to',
        '--dateTo', help='Return videos before specified time, as yyyy-mm-ddTHH:MM:SS.sssZ')
    subparser_list.add_argument('-q', '--quality', default='ask', help="Quality of the videos to use. Usually should be LOW, 1500, 5000, UHD. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_list.add_argument('-ext', '--extension', default="mp4", help="File extension to search. Deapult to 'mp4'. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_list.add_argument('-s', '--statistics', action="store_true",
                        help='Fetch and save video durations and file sizes.')

    subparser_list.add_argument('-o', '--output', default="videos.csv",
                            help="File name to output video filenames. Default 'videos.csv'")
    subparser_list.set_defaults(func=flist)

    # List Batch command
    subparser_blist = subparsers.add_parser(
        'batch', help="List video files based on parameters stored in a csv file.")
    subparser_blist.add_argument('token', help='API token')
    subparser_blist.add_argument('csvfile', help='Csv file with arguments to interate')

    subparser_blist.add_argument('-q', '--quality', default='ask', help="Quality of the videos to use. Usually should be LOW, 1500, 5000, UHD. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_blist.add_argument('-ext', '--extension', default="mp4", help="File extension to search. Deapult to 'mp4'. \
        'ask' will list avaiable options and ask user to choose one. 'all' will get all avaiable videos.")
    subparser_blist.add_argument('-s', '--statistics', action="store_true",
                        help='Fetch and save video durations and file sizes.')
    subparser_blist.add_argument('-o', '--output', default="videos.csv",
                        help="File name to output video filenames. Default 'videos.csv'")
    subparser_blist.set_defaults(func=fbatch)

    # get dives
    subparser_dives = subparsers.add_parser(
        'getDives', help="Create a csv file listing dives from Oceans3.0")
    subparser_dives.add_argument('token', help='API token')
    subparser_dives.add_argument('-o', '--output', default="dives.csv",
                            help="File name to output dives. Default 'dives.csv'")
    subparser_dives.add_argument('-l', '--nolocation', action="store_false",
                            help='Do not get locationCodes from dives')
    subparser_dives.set_defaults(func=fgetDives)

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
        'download', help="Download video files")
    subparser_download.add_argument(
        'input', help="A csv file with list of archived filenames (output of 'list' command).")
    subparser_download.add_argument('-o', '--output', default="output",
                            help="Folder to download files. Default 'output'")
    subparser_download.add_argument('-t', '--trim', action="store_true", help='')
    subparser_download.set_defaults(func=fdownload)

    # extract Frame
    subparser_extframe = subparsers.add_parser(
        'extframe', help="Extract frames from video files")
    subparser_extframe.add_argument('input', help=help_input)
    subparser_extframe.add_argument('interval', type=float,
                            help="Get frames every 'X' seconds. Default to 1 second.")
    subparser_extframe.add_argument('-o', '--output', default="frames",
                            help="Folder to download frames. Default 'frames'")
    subparser_extframe.add_argument('-t', '--trim', action="store_true", help='')
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
    subparser_extframe.set_defaults(func=fextFOV)

    args = parser.parse_args()
    args.func(args)





# TO DO
# Download from seatube pro and seatubeV3
# download NAV and CTD files from same site
# extract frame from multiple FOV

# https://data.oceannetworks.ca/SeaTube?resourceTypeId=1000&resourceId=66500&diveId=158785978&time=2023-07-02T03:14:15.000Z
# https://data.oceannetworks.ca/SeaTube?resourceTypeId=1000&resourceId=74260&diveId=163865871&time=2023-09-08T00:37:57.000Z
# https://data.oceannetworks.ca/SeaTube?resourceTypeId=1000&resourceId=74260&diveId=163953846&time=2023-09-09T00:15:38.000Z

# https://data.oceannetworks.ca/SeaTubeV3?resourceTypeId=600&resourceId=5590&time=2021-08-23T04:46:32.000Z


# args = parser.parse_args(['list','--token','c1416a5f-2dc7-4cc6-83f0-17a8261f9826','--deviceCode','AXISCAMB8A44F04DEEA'])
# args


# args, unknown = parser.parse_known_args(['getDives','--token','c1416a5f-2dc7-4cc6-83f0-17a8261f9826','--deviceCode','AXISCAMB8A44F04DEEA'])
# args
if __name__ == '__main__':
    main()
