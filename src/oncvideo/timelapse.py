"""Function to make time-lapses"""

from pathlib import Path
import copyreg
import subprocess as sp
import tempfile
import json
from shutil import copyfile
from tqdm.auto import tqdm
import numpy as np
import pandas as pd
import cv2
from PIL import Image, ImageDraw
from ttf_opensans import opensans
from ._utils import LOGO, strfdelta
from .utils import name_to_timestamp


def _pickle_keypoints(point):
    return cv2.KeyPoint, (*point.pt, point.size, point.angle,
                          point.response, point.octave, point.class_id)

copyreg.pickle(cv2.KeyPoint().__class__, _pickle_keypoints)


def make_timelapse(folder='fovs', time_display='elapsed', time_format=None, time_offset=0, fps=10,
    font_size=44, logo=False, caption=None, time_xy=None, caption_xy=None, **kwargs):
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
    font_size : float, default 44
        Font size for the timestamp and caption.
    logo : bool, default False
        Include ONC logo on the video?
    caption : str, default None
        Insert a caption at the bottom of the screen. You can break lines with <br> tag.
    time_xy : tuple of 2 ints, default None
        Coordinates of the bottom-left corner of the time text. X is the distance (in pixels) from the left edge and
        Y is the distance from the top edge of the image. Default will draw in the top-left corner.
    caption_xy : tuple of 2 int, default None
        Coordinates of the bottom-left corner of the first line of the caption. Default will draw in the bottom corner.
    **kwargs
        Arguments passed to other function. Possible options are 'font_weight' (ttf_opensans.opensans) and 'fill', 'spacing',
        'anchor' and 'align' (PIL.ImageDraw.text).
    """
    defaultKwargs = dict(
        fill=None,
        anchor=None,
        spacing=4,
        align='left',
        font_weight=400
    )
    
    kwargs = {**defaultKwargs, **kwargs}

    open_sans = opensans(font_weight=kwargs['font_weight'], italic=False)
    font = open_sans.imagefont(size=font_size)

    folder = Path(folder)

    if not folder.exists():
        raise ValueError(f"Folder {folder} not found.")

    fu = [f for f in folder.iterdir() if f.is_dir()]
    if len(fu) == 0:
        fu = [folder]

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
        video_dim = img_ref.shape[::-1]
        
        tmpfile = tempfile.gettempdir() / output_video
        vidwriter = cv2.VideoWriter(str(tmpfile), cv2.VideoWriter_fourcc(*"mp4v"), fps, video_dim)

        spacing = img_ref.shape[0] // 20 # 5% of the image size

        if time_xy is None:
            txy = (spacing, spacing)
            tanchor = None
            talign = "left"
        else:
            txy = tuple(time_xy)
            tanchor = kwargs['anchor']
            talign = kwargs['align']


        if caption_xy is None:
            cxy = (img_ref.shape[0]-spacing, img_ref.shape[1] // 2)
            canchor = 'md'
            calign = 'middle'
        else:
            cxy = tuple(caption_xy)
            canchor = kwargs['anchor']
            calign = kwargs['align']

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

            # img = cv2.imread(str(imgfile), cv2.IMREAD_COLOR)

            img = Image.open(imgfile)  
            draw = ImageDraw.Draw(img)
            

            # format timestamp of time lapsed string
            if do_time:
                timestamp = name_to_timestamp(imgfile.name)

                if time_display == 'elapsed':
                    timedelta = timestamp - timestamp0 + time_offset
                    timestamp = strfdelta(timedelta, time_format)
                else:
                    timestamp = timestamp.strftime(time_format)

                # Using cv2.putText() method
                draw.text(txy, timestamp, fill=kwargs['fill'], font=font, anchor=tanchor,
                          align=talign, spacing=kwargs['spacing'])

            if caption is not None:
                draw.text(cxy, caption,fill=kwargs['fill'], font=font, anchor=canchor,
                          align=calign, spacing=kwargs['spacing'])
            
            # convert Pillow to OpenCV
            img = np.array(img)
            img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

            # insert logo
            if logo:
                # destination = img[top_y:bottom_y, left_x:right_x]
                # result = cv2.addWeighted(destination, 1, logo_resize, 0.5, 0)
                # img[top_y:bottom_y, left_x:right_x] = result
                img[top_y:bottom_y, left_x:right_x] = logo_resize

            vidwriter.write(img)

        vidwriter.release()

        if append:
            # i = 1
            old_video = output_video.rename(Path(tempfile.gettempdir()) / 'old_video.mp4')
            # while output_video.exists():
            #     output_video = output_video.with_stem(f"{output_video.stem}_V{i}")
            #     i += 1
            
            list_file = Path(tempfile.gettempdir()) / 'file_list.txt'

            metadata = {'First frame': first_last['First frame'], 'Last frame': images[-1].name}

            with open(list_file, "w") as f:
                f.write(f"file '{old_video.resolve()}'\n")
                f.write(f"file '{tmpfile}'\n")

            cmd = ['ffmpeg', '-v', 'quiet', '-f', 'concat', '-safe', '0', '-i', list_file,
                '-metadata', f"comment={json.dumps(metadata)}",
                '-c', 'copy', output_video]
            
            sp.run(cmd, check=True)
            tmpfile.unlink()
            old_video.unlink()
            list_file.unlink()

        else:
            metadata = {'First frame': images[0].name, 'Last frame': images[-1].name}

            cmd = ['ffmpeg', '-v', 'quiet', '-i', tmpfile, 
                '-metadata', f"comment={json.dumps(metadata)}", 
                '-c', 'copy', output_video]

            sp.run(cmd, check=True)
            tmpfile.unlink()


def align_frames(folder='fovs', method='ORB+ECC', reference='middle', align_matrix=None, **kwargs):
    """
    Align frames

    This function is to be used before 'make_timelapse'. It will go in each folder (FOV) and align the
    frames based in a reference image, so you can create a smoother timelapse. Aligned frames are saved
    in separate folder with the suffix 'aligned'.There are two alignment types supported: Feature Matching (ORB/SIFT) 
    and ECC Image Alignment, both implemented in openCV.
    ORB Feature Matching finds key points to align images and can handle large misalignments (translation, rotation, scale),
    but might not be precise. ECC can generate more precise alignment, but its much slower and often fails if there
    are large misalignments.

    Parameters
    ----------
    folder : str, default 'fovs'
        Path to a folder where .jpg images are stored.
    method : {'ORB', 'ECC', 'ORB+ECC'}, default ORB+ECC
        Algorithm used for alignment: Feature Matching (ORB) or ECC Image Alignment.
        'ORB+ECC' will perform ORB followed by ECC.
    reference : {'first', 'middle', 'last', 'previousX'}, str or list of strs, default 'middle'
        Define the reference frame which other frames will be aligned to. Can define as the first, middle, or last frame
        of the folder, in chronological order. 'previousX' will update the reference image every X-th aligned image.
        For example, 'previous1' will update the reference every image, 'previous20' will update the reference every 20 images.
        Alternatively, you can define the filename of the image to be used as
        reference. Can be also a list of filenames that can be used as reference for each subfolder inside 'folder'.
    align_matrix : a 2x3 matrix of np.float32 type
        Perform an additional transformation after the alignment. Useful if trying to center or zoon in the images.
        I suggest you use the 'Unified Transform' tool from GIMP to get the transform matrix.
    **kwargs
        Arguments passed to other function. Possible options are 'nfeatures' (cv2.ORB_create), 'mask' (detectAndCompute
        from cv2.ORB_create), indexParams and searchParams (cv2.FlannBasedMatcher) and epsilon and maxCount
        (termination criteria for cv2.findTransformECC).
    """
    defaultKwargs = dict(
        nfeatures = 5000,
        epsilon = 1e-8, # 1e-5 and 1e-10
        maxCount = 500, # 1000 - 5000
        indexParams = dict(algorithm=6, table_number=6, key_size=12, multi_probe_level=1),
        searchParams = dict(checks=50),
        mask = None
    )
    
    kwargs = {**defaultKwargs, **kwargs}

    input_folder = Path(folder)

    if not input_folder.exists():
        raise ValueError(f"Folder {folder} not found.")

    fu = [f for f in input_folder.iterdir() if f.is_dir()]
    output_folder = Path(folder + '_aligned')
    output_folder.mkdir(exist_ok=True)

    if len(fu) == 0:
        align_matrix = [align_matrix]
        fu = [input_folder]
        fo = [output_folder]
    else:
        fo = [output_folder / f.name for f in fu]

        # create output subfolder
        for out in fo:
            out.mkdir(exist_ok=True)

    criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, kwargs['maxCount'], kwargs['epsilon'])
    
    if method == 'ORB':
        orb = True
        ecc = False
    elif method == 'ECC':
        orb = False
        ecc = True
    elif method == 'ORB+ECC':
        orb = True
        ecc = True
    else:
        raise ValueError("'method' must be either 'ORB', 'ECC', or 'ORB+ECC'")
    

    manual_transform = align_matrix is not None
    if isinstance(align_matrix, list):
        if len(fu) != len(align_matrix):
            raise ValueError("If a list, 'align_matrix' must have same size as number of subfolder inside 'folder'.")
    else:
        align_matrix = [align_matrix] * len(fu)
    
    if isinstance(reference, list):
        if len(fu) != len(reference):
            raise ValueError("If a list, 'reference' must have same size as number of subfolder inside 'folder'.")
    else:
        reference = [reference] * len(fu)

    orbd = cv2.ORB_create(kwargs['nfeatures'])  # ORB detector

    # FLANN-based matcher
    matcher = cv2.FlannBasedMatcher(kwargs['indexParams'], kwargs['searchParams'])

    for f, out, wmat, ref in zip(tqdm(fu, desc='Processed folders'), fo, align_matrix, reference):
        images = f.glob("*.jpg")
        images = sorted(images)
        if len(images) < 1:
            continue

        # open reference image
        update_reference = False
        copy_ref = True
        if ref == 'first':
            imgfile = images[0]
        elif ref == 'last':
            imgfile = images[-1]
        elif ref == 'middle':
            imgfile = images[len(images) // 2]
        elif ref.startswith('previous'):
            imgfile = images[0]
            update_reference = True
            nupdate = int(ref.replace('previous',''))
        else:
            imgfile = f / ref
            if not imgfile.exists():
                imgfile = Path(ref)
                copy_ref = False
            if not imgfile.exists():
                print(f"Skipping folder '{f}', reference image '{ref}' not found.")
                continue

        img_ref = cv2.imread(str(imgfile), cv2.IMREAD_COLOR)

        # copy reference image into the output
        if copy_ref:
            images.remove(imgfile)
            if manual_transform:
                sz = (img_ref.shape[1], img_ref.shape[0])
                img_algn = cv2.warpAffine(img_ref, wmat, sz, flags=cv2.INTER_LINEAR)
                cv2.imwrite(str(out / imgfile.name), img_algn, [cv2.IMWRITE_JPEG_QUALITY, 100])
            else:
                copyfile(imgfile, out / imgfile.name)

        img_ref = cv2.cvtColor(img_ref, cv2.COLOR_BGR2GRAY)
        kp1, des1 = orbd.detectAndCompute(img_ref, kwargs['mask'])
        
        i = 0
        for imgfile in tqdm(images, leave=False):
            outimg = out / imgfile.name
            if outimg.exists():
                continue
            
            img = cv2.imread(str(imgfile), cv2.IMREAD_COLOR)
            img_gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
            sz = (img.shape[1], img.shape[0])

            # ===== 1. FEATURE-BASED ALIGNMENT USING ORB =====
            if orb:
                kp2, des2 = orbd.detectAndCompute(img_gray, kwargs['mask'])

                matches = matcher.knnMatch(des1, des2, k=2)

                # Apply Lowe's Ratio Test to filter good matches
                matches = [m for m in matches if len(m) == 2]
                good_matches = [m for m, n in matches if m.distance < 0.75 * n.distance]

                if len(good_matches) >= 10:
                    src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                    dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)

                    # Compute Homography
                    M , _= cv2.estimateAffinePartial2D(dst_pts, src_pts)
                    # M, _ = cv2.findHomography(dst_pts, src_pts, cv2.RANSAC, 5.0)

                    # constrain translation and scale only
                    scale_x = np.linalg.norm(M[:, 0])  # Scale factor along X
                    scale_y = np.linalg.norm(M[:, 1])  # Scale factor along Y
                    scale = (scale_x + scale_y) / 2  # Average scale
                    M = np.array([[scale, 0, M[0, 2]],
                                  [0, scale, M[1, 2]]], dtype=np.float32)
                    
                    img_algn = cv2.warpAffine(img, M, sz)
                    # img_algn = cv2.warpPerspective(img, M, sz)
                else:
                    print(f"Skiping file {imgfile}. Not enough matches found.")
                    continue

            # ===== 2. FINE-TUNE ALIGNMENT USING ECC =====
            if ecc:
                img_gray = cv2.cvtColor(img_algn, cv2.COLOR_BGR2GRAY)

                # ECC Image Alignment
                warp_matrix = np.eye(3, 3, dtype=np.float32)
                _, warp_matrix = cv2.findTransformECC(img_ref, img_gray, warp_matrix, cv2.MOTION_HOMOGRAPHY, criteria)

                img_algn = cv2.warpPerspective(img_algn, warp_matrix, sz, flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP)

            # Perform manual transformation if requested
            if manual_transform:
                img_algn = cv2.warpAffine(img_algn, wmat, sz, flags=cv2.INTER_LINEAR)

            cv2.imwrite(str(outimg), img_algn, [cv2.IMWRITE_JPEG_QUALITY, 100])

            # update reference
            if update_reference:
                i += 1
                if i % nupdate == 0:
                    img_ref = cv2.cvtColor(img_algn, cv2.COLOR_BGR2GRAY)
                    kp1, des1 = orbd.detectAndCompute(img_ref, None)





