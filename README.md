# ONCvideo

A collection of tools to help get archived videos from Oceans3.

Avaiable commands include:

* list - List video files for a specific camera between two dates
* batch - List video files based on parameters stored in a csv file.
* getDives - Create a csv file listing dives from Oceans3.0
* info - Extract video information (duration, resolution, fps)
* didson - Extract information of DIDSON files
* download - Download video files
* extframe - Extract frames from video files
* extfov - Extract FOVs (frames or videos) from video files

## Installation
```
git clone https://github.com/correapvf/oncvideo
python setup.py install
```
FFmpeg must be installed and accessible via the `$PATH` environment variable.

After installation, use `oncvideo -h` to get help and a list of available options.
