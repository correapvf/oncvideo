# ONCvideo

A collection of commands to help get archived videos from Oceans 3.

Avaiable commands include:

* list - List video files for a specific camera between two dates
* blist - List video files based on parameters stored in a csv file
* getDives - Create a csv file listing dives from Oceans3.0
* info - Extract video information (duration, resolution, fps)
* didson - Extract information of DIDSON files
* download - Download video files
* tomp4 - Convert video to mp4 format
* extframe - Extract frames from video files
* extfov - Extract FOVs (frames or videos) from video files
* make_timelapse - Generate timelapse video from images
* downloadTS - Download time series data
* mergeTS - Merge time series data based on the closest timestamps
* downloadST - Download video from Seatube link
* linkST - Generate link video from Seatube
* renameST - Rename framegrabs from Seatube to correct timestamp

## Installation
```
pip install oncvideo
```
> Before using `ONCvideo`, FFmpeg must be installed and accessible via the `$PATH` environment variable.
Check https://ffmpeg.org/download.html on details how to install, or use your package manager of choice (e.g. `sudo apt install ffmpeg` on Debian/Ubuntu, `brew install ffmpeg` on OS X, `winget install ffmpeg` on Windows).

## Getting started
The package provides multiple commands to get a list of available videos in our archives and perform multiple operations with the videos (e.g. downloading, extracting frames, etc).

In most use cases, you will use commands `list` or `blist` to get a list of files based on your search criteria. This will generate a csv file that you can use in R, Python or Excel to check the availability of data, remove bad files (size or duration too small) or filter files based a criteria that you want. For example, you may want to filter one file every day/week/month and download these videos to check its quality throughout your time range. For example:

```
oncvideo list -t API_TOKEN -lc BACAX -from 2018-01-01 -to “2024-05-01 04:00:00”
```
- API_TOKEN: this is your API_TOKEN. You can obtain one logging in https://data.oceannetworks.ca > Profile > Web Service API
- BACAX: that is the location code for Barkley Axis. An easy way to find the location code is to select the site you want in the Data Search (https://data.oceannetworks.ca/DataSearch) and check in the URL the value after *locationCode=*
- 2018 and “2024-05-01 04:00:00” is the time range that you can specify the search. Note the quotes when you have space.

You can also use -h to see a list of possible arguments for each subcommand:
```
oncvideo list -h
```

`list` can also get videos based on deviceID or deviceCode, or based on dives number (for ROVs only). Check `blist` in case you need to perform multiple searchs for a giver project (e.g. multiple surveys of ROV dives, a given month at different sites).

After filtering the csv file, you can use other commands. E.g. to extract frames:
```
oncvideo extfov FILTERED.csv -s 00:30,01:45,02:30
```
- FILTERED.csv is the name of your csv file
- 00:30,01:45,02:30 are the timestamps to extract frames (you can separate multiple with comma). **Alternatively**, you can create a column named `fovs` and put timestamps in that column. The advantage is that you can use different timestamps for different times/periods, as different deployments may have different schedules for the cameras.

If you are interested, you can create a video time-lapse based on the frames that your extracted using the subcommand `timelapse`.


## Docs

Documentation for functions in the package is avaiable at https://correapvf.github.io/oncvideo.
After installation, use `oncvideo -h` to get help and a list of available commands avaiable as well.

See also [tests](tests) folder for exemples of *.csv* files that can be used with the *blist*/*list_file_batch* command/function.


## Setting API key

If you use the commands often, it may get boring to type the API key every time. You can create an enviroment variable in your OS named `ONC_API_TOKEN` and store your API key there. The package will automatically get the API key and you don't need to provide as an argument.
