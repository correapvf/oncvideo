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
* downloadTS - Download time series data
* mergeTS - Merge time series data based on the closest timestamps
* downloadST - Download video from Seatube link
* linkST - Generate link video from Seatube
* renameST - Rename framegrabs from Seatube to correct timestamp

## Installation
```
pip install git+https://github.com/correapvf/oncvideo
```
FFmpeg must be installed and accessible via the `$PATH` environment variable.

## Docs

Documentation for functions in the package is avaiable at http://correapvf.github.io/oncvideo.
After installation, use `oncvideo -h` to get help and a list of available commands avaiable as well.

See also [tests](tests) folder for exemples of *.csv* files that can be used with the *blist*/*list_file_batch* command/function.
