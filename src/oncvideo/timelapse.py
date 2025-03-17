"""Function to make time-lapses"""

from pathlib import Path
import subprocess as sp
import tempfile
import json
from shutil import copyfile
from tqdm.auto import tqdm
import numpy as np
import pandas as pd
import cv2
from ._utils import LOGO, strfdelta
from .utils import name_to_timestamp


def make_timelapse(folder='fovs', time_display='elapsed', time_format=None, time_offset=0, fps=10,
    fontScale=1, logo=False, caption=None, time_xy=None, caption_xy=None):
    """
    Generate timelapse video from images

    Parameters
    ----------
    folder : str, default 'fovs'
        Path to a folder where .jpg images are stored.
    time_display : {'elapsed', 'current', 'none'}
        How to print the time on the frame. 'elapsed' will display as elapsed time since first
        frame, offset by 'time_offset'. 'current' will display the current real time of the frame.
        'none' will not display time.
    time_format : str, default '%Y/%m/%d %Hh' if time_display='current', and '%d days %{H}h' if time_display='elapsed'
        Format how the timestamp will be written on the video. For time_display='current', check formatting options for
        'strftime'. For time_display='elapsed', options are %y %m %w %d %H %M %S for years, months,
        weeks, days, hours, minutes, seconds.
    time_offset : str, timedelta or float, default 0
        Offset the time displayed in the frame if time_display='elapsed'.
        Passed to pd.to_timedelta, check it's documentation for options.
    fps : float, default 10
        Timelapse video FPS.
    fontScale : float, default 1
        Font scale for the timestamp. Increase values for a larger font size.
    logo : bool, default False
        Include ONC logo on the video?
    caption : str, default None
        Insert a caption at the bottom of the screen. You can break lines with <br> tag.
    time_xy : tuple of 2 ints, default None
        Coordinates of the bottom-left corner of the time text. X is the distance (in pixels) from the left edge and
        Y is the distance from the top edge of the image. Default will draw in the top-left corner.
    caption_xy : tuple of 2 int, default None
        Coordinates of the bottom-left corner of the first line of the caption. Default will draw in the bottom corner.
    """
    folder = Path(folder)

    if not folder.exists():
        raise ValueError(f"Folder {folder} not found.")

    fu = [f for f in folder.iterdir() if f.is_dir()]
    fu += [folder]

    if logo:
        logoimg = cv2.imdecode(np.frombuffer(LOGO, np.uint8), cv2.IMREAD_COLOR)

    if time_display not in ['elapsed', 'current', 'none']:
        raise ValueError("'time_display' must be one of 'elapsed', 'current' or 'none'")

    do_time = False if time_display == 'none' else True

    if do_time:
        time_offset = pd.to_timedelta(time_offset)

        if time_format is None:
            time_format = '%Y/%m/%d %Hh' if time_display == 'current' else '%d days %{H}h'

    for f in tqdm(fu, desc='Processed folders'):
        images = f.glob("*.jpg")
        images = sorted(images)
        if len(images) < 1:
            continue

        if do_time:
            timestamp0 = name_to_timestamp(images[0].name)

        imgfile = images[0]

        # if the videos exists, will try to append
        output_video = Path(f.name + '.mp4')
        if output_video.exists():
            ffprobe_cmd = ['ffprobe', '-v', 'quiet',
                    '-select_streams', 'v:0',
                    '-show_format',
                    '-of', 'json',
                    '-i', output_video]
        
            out_raw = sp.check_output(ffprobe_cmd)
            out_dict = json.loads(out_raw)
            first_last = json.loads(out_dict['format']['tags']['comment'])

            index = next(i for i, img in enumerate(images) if img.name == first_last['Last frame']) + 1
            images = images[index::]

            append = True

            if len(images) == 0:
                print(f"Skkiping file {output_video.name}. File already exists and no new frames to add.")
                continue
        else:
            append = False

        # read one frame to get img size
        img_ref = cv2.imread(str(imgfile), cv2.IMREAD_GRAYSCALE)
        img_ref_name = imgfile.name
        video_dim = img_ref.shape[::-1]
        
        tmpfile = tempfile.gettempdir() / output_video
        vidwriter = cv2.VideoWriter(str(tmpfile), cv2.VideoWriter_fourcc(*"mp4v"), fps, video_dim)

        spacing = img_ref.shape[0] // 40 # 2.5% of the image size
        ctxt = (spacing, spacing+int(22*fontScale)) if time_xy is None else tuple(time_xy)
        font = cv2.FONT_HERSHEY_SIMPLEX

        if logo:
            size_logo = img_ref.shape[0] // 7 # 7.5% of the image size
            logo_resize = cv2.resize(logoimg, (size_logo,size_logo), interpolation=cv2.INTER_LINEAR)

            # top right corner
            top_y = spacing
            left_x = img_ref.shape[1] - spacing - size_logo
            bottom_y = spacing + size_logo
            right_x = img_ref.shape[1] - spacing


        # Start loop for each image
        for imgfile in tqdm(images, leave=False):

            img = cv2.imread(str(imgfile), cv2.IMREAD_COLOR)

            # format timestamp of time lapsed string
            if do_time:
                timestamp = name_to_timestamp(imgfile.name)

                if time_display == 'elapsed':
                    timedelta = timestamp - timestamp0 + time_offset
                    timestamp = strfdelta(timedelta, time_format)
                else:
                    timestamp = timestamp.strftime(time_format)

                # Using cv2.putText() method
                img = cv2.putText(img, timestamp, org=ctxt, fontFace=font,
                                fontScale=fontScale, color=(255, 255, 255), thickness=2, lineType=cv2.LINE_AA)

            if caption is not None:
                textY = img.shape[0]-spacing if caption_xy is None else caption_xy[1]
                for line in reversed(caption.split('<br>')):
                    textsize = cv2.getTextSize(line, font, fontScale, thickness=2)[0]
                    textX = (img.shape[1] - textsize[0]) // 2 if caption_xy is None else caption_xy[0]
                    img = cv2.putText(img, line, org=(textX, textY), fontFace=font,
                                fontScale=fontScale, color=(255, 255, 255), thickness=2, lineType=cv2.LINE_AA)
                    textY = textY - textsize[1] - spacing

            # insert logo
            if logo:
                # destination = img[top_y:bottom_y, left_x:right_x]
                # result = cv2.addWeighted(destination, 1, logo_resize, 0.5, 0)
                # img[top_y:bottom_y, left_x:right_x] = result
                img[top_y:bottom_y, left_x:right_x] = logo_resize

            vidwriter.write(img)

        vidwriter.release()

        if append:
            i = 1
            old_video = output_video
            while output_video.exists():
                output_video = output_video.with_stem(f"{output_video.stem}_V{i}")
                i += 1
            
            list_file = Path(tempfile.gettempdir()) / 'file_list.txt'

            metadata = {'First frame': first_last['First frame'], 'Last frame': images[-1].name}

            with open(list_file, "w") as f:
                f.write(f"file '{old_video.resolve()}'\n")
                f.write(f"file '{tmpfile}'\n")

            cmd = ['ffmpeg', '-v', 'quiet', '-f', 'concat', '-safe', '0', '-i', list_file,
                '-metadata', f"comment={json.dumps(metadata)}",
                '-c', 'copy', output_video]

        else:
            metadata = {'First frame': images[0].name, 'Last frame': images[-1].name}

            cmd = ['ffmpeg', '-v', 'quiet', '-i', tmpfile, 
                '-metadata', f"comment={json.dumps(metadata)}", 
                '-c', 'copy', output_video]

        sp.run(cmd, check=True)
        tmpfile.unlink()



def align_frames(folder='fovs', reference='middle', warp_mode='perspective', epsilon=1e-6, max_iterations=3000):
    """
    Align frames

    This function is to be used before 'make_timelapse'. It will go in each folder (FOV) and align the
    frames based in a reference image, so you can create a smoother timelapse. Aligned frames are saved
    in separate folder with the suffix 'aligned'. The function uses ECC Image Alignment implemented in openCV.
    Might need to tune epsilon and max_iterations or change warp_mode to get better results. If the timelapse
    has biofouling, you might split the frames manually and run the alignment separately. 

    Parameters
    ----------
    folder : str, default 'fovs'
        Path to a folder where .jpg images are stored.
    reference : {'first', 'middle', 'last'} or str, default 'middle'
        Define the reference frame which other frames will be aligned to. Can define as the first, middle, or last frame
        of the folder, in chronological order. Alternativally, you can define the filename of the image to be used as
        reference. Image must be inside the folder.
    warp_mode : {'affine', 'perspective'}, default affine
        affine - allows translation, rotation and scale transformations in the images only
        perspective - allows perspective transformations in addition to translation, rotation and scale 
    epsilon : float, default 1e-6
        Minimum acceptable error difference between iterations. Smaller values lead to better
        precision but can take longer. Typical range between 1e-5 and 1e-10.
    max_iterations : int, default 3000
        Maximum number of times the algorithm updates the transformation matrix. You can increase it to get a better
        alignment, but it might take longer. Typical values between 1000–5000.
    """
    input_folder = Path(folder)

    if not input_folder.exists():
        raise ValueError(f"Folder {folder} not found.")

    fu = [f for f in input_folder.iterdir() if f.is_dir()]
    output_folder = Path(folder + '_aligned')
    fo = [output_folder / f.name for f in fu]

    # create output folder
    output_folder.mkdir(exist_ok=True)
    for out in fo:
        out.mkdir(exist_ok=True)

    fu += [input_folder]
    fo += [output_folder]

    criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, max_iterations, epsilon)

    match warp_mode:
        case 'affine':
            warp_mode = cv2.MOTION_AFFINE
            nrows = 2
            warp_function = cv2.warpAffine
        case 'perspective':
            warp_mode = cv2.MOTION_HOMOGRAPHY
            nrows = 3
            warp_function = cv2.warpPerspective
        case _:
            raise ValueError("'warp_mode' must be a string either 'affine' or 'perspective'")
    
    for f, out in zip(tqdm(fu, desc='Processed folders'), fo):
        images = f.glob("*.jpg")
        images = sorted(images)
        if len(images) < 1:
            continue

        # open reference image
        match reference:
            case 'first':
                imgfile = images[0]
            case 'last':
                imgfile = images[-1]
            case 'middle':
                imgfile = images[len(images) // 2]
            case _:
                imgfile = f / reference
                if not imgfile in images:
                    print(f"Skipping folder '{f}', reference image '{reference}' not found.")
                    continue

        img_ref = cv2.imread(str(imgfile), cv2.IMREAD_GRAYSCALE)

        # copy reference image into the output
        copyfile(imgfile, out / imgfile.name)
        images.remove(imgfile)

        for imgfile in tqdm(images, leave=False):

            # skip image if there is an aligned image in the output folder
            outimg = out / imgfile.name
            if outimg.exists():
                continue

            img = cv2.imread(str(imgfile), cv2.IMREAD_COLOR)
            img_gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
            sz = (img.shape[1], img.shape[0])

            # ECC Image Alignment
            warp_matrix = np.eye(nrows, 3, dtype=np.float32)
            _, warp_matrix = cv2.findTransformECC(img_ref, img_gray, warp_matrix, warp_mode, criteria)

            img_algn = warp_function(img, warp_matrix, sz, flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP)

            cv2.imwrite(str(outimg), img_algn, [cv2.IMWRITE_JPEG_QUALITY, 100])
