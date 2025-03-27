from pathlib import Path
import pandas as pd
from oncvideo._arg_parser import main as parser
from oncvideo.seatube import rename_st

class TestDownloadSeatube():
    def setup_class(self):
        parser([
                "downloadST",
                "tests/seatube_test.csv",
                # "-t",
                # "API key",
                "-e",
                "mov"
              ])
        self.df = pd.read_csv("videos_seatube.csv")
    
    def test_shape(self):
        assert self.df.shape == (1, 5)

    def test_video(self):
        assert self.df.loc[0, 'url'] == "https://data.oceannetworks.ca/AdFile?filename=INSPACMINIZEUS4KCAMODYSSEUS_20220728T174133.000Z.mov"

    def test_frame(self):
        assert self.df.loc[0, 'frame_filename'] == "INSPACMINIZEUS4KCAMODYSSEUS_20220728T174951.009Z.jpg"

    def teardown_class(self):
        Path('videos_seatube.csv').unlink()


class TestLink():
    def setup_class(self):
        parser([
                "linkST",
                "tests/videos_test.csv",
                # "-t",
                # "API key",
              ])
        self.df = pd.read_csv("output_link.csv")

    def test_shape(self):
        assert self.df.shape == (4,  13)

    def test_url(self):
        assert not self.df['url'].isnull().any()

    def teardown_class(self):
        Path('output_link.csv').unlink()


class TestRename():
    def test_rename1(self):
        newname = rename_st('DEVICECODE_20230913T200203.000Z-003.jpeg')
        assert newname == 'DEVICECODE_20230913T200201.000Z.jpeg'

    def test_rename2(self):
        newname = rename_st('INSPACMINIZEUS4KCAMODYSSEUS_20220728T175017.000Z-009.jpg')
        assert newname == 'INSPACMINIZEUS4KCAMODYSSEUS_20220728T175021.000Z.jpg'
    
    def test_invalid(self):
        newname = rename_st('DEVICECODE_20220728T175017.000Z-5000.jpeg')
        assert newname == 'DEVICECODE_20220728T175017.000Z-5000.jpeg'
