from pathlib import Path
import pandas as pd
from oncvideo._arg_parser import main as parser
from oncvideo.seatube import st_rename

class TestDownloadSeatube():
    def setup_class(self):
        parser([
                "downloadST",
                "https://data.oceannetworks.ca/SeaTubeV3?resourceTypeId=600&resourceId=3851&time=2022-07-28T17:49:51.009Z",
                "-t",
                "c1416a5f-2dc7-4cc6-83f0-17a8261f9826",
                "-ext",
                "mp4",
                "-f"
              ])

    def test_video(self):
        p = Path('INSPACMINIZEUS4KCAMODYSSEUS_20220728T174502.000Z-LOW.mp4')
        assert p.is_file()

    def test_frame(self):
        p = Path('INSPACMINIZEUS4KCAMODYSSEUS_20220728T174951.009Z.jpg')
        assert p.is_file()

    def teardown_class(self):
        Path('INSPACMINIZEUS4KCAMODYSSEUS_20220728T174502.000Z-LOW.mp4').unlink()
        Path('INSPACMINIZEUS4KCAMODYSSEUS_20220728T174951.009Z.jpg').unlink()


class TestLink():
    def setup_class(self):
        parser([
                "linkST",
                "tests/videos_test.csv",
                "-t",
                "c1416a5f-2dc7-4cc6-83f0-17a8261f9826"
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
        newname = st_rename('DEVICECODE_20230913T200203.000Z-003.jpeg')
        assert newname == 'DEVICECODE_20230913T200201.000Z.jpeg'

    def test_rename2(self):
        newname = st_rename('INSPACMINIZEUS4KCAMODYSSEUS_20220728T175017.000Z-009.jpg')
        assert newname == 'INSPACMINIZEUS4KCAMODYSSEUS_20220728T175021.000Z.jpg'
    
    def test_invalid(self):
        newname = st_rename('DEVICECODE_20220728T175017.000Z-5000.jpeg')
        assert newname == 'DEVICECODE_20220728T175017.000Z-5000.jpeg'
