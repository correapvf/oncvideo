from pathlib import Path
import shutil
import pytest
import pandas as pd
from oncvideo._arg_parser import main as parser

class TestDownload():
    def setup_class(self):
        parser([
                "download",
                "tests/videos_test.csv",
                "-t",
                "-o",
                "videos_trim"
              ])
        self.df = pd.read_csv("videos_trim/videos_trim.csv")

    def test_shape(self):
        assert self.df.shape == (4, 4)

    def test_filename(self):
        assert self.df.iloc[0,1] == 'INSPACMINIZEUS4KCAMODYSSEUS_20220729T054917.000Z-1500.mp4'

    def test_group(self):
        assert self.df['subfolder'].nunique() == 2

    def test_files1(self):
        p = Path('videos_trim/VS000169').glob('*.mp4')
        assert len(list(p)) == 3

    def test_files2(self):
        p = Path('videos_trim/VS000170').glob('*.mp4')
        assert len(list(p)) == 1

    def teardown_class(self):
        shutil.rmtree("videos_trim")


def finalizer_function():
    Path("videos.csv").unlink()
    shutil.rmtree("videos")

@pytest.fixture(scope="session", autouse=True)
def setup_session(request):
    parser([
        "download",
        "tests/videos_test.csv",
        "-o",
        "videos"
        ])
    # prepare something ahead of all tests
    request.addfinalizer(finalizer_function)


class TestTomp4():
    def setup_class(self):
        parser([
                "tomp4",
                "videos/VS000169/INSPACMINIZEUS4KCAMODYSSEUS_20220729T054221.000Z-1500.mp4",
              ])

    def test_csv(self):
        df = pd.read_csv("output/output.csv")
        assert df.shape == (1, 3)

    def test_file(self):
        p = Path('output').glob('*.mp4')
        assert len(list(p)) == 1

    def teardown_class(self):
        shutil.rmtree("output")


class TestInfo():
    def setup_class(self):
        parser([
                "info",
                "videos/*.mp4"
              ])
        self.df = pd.read_csv("video_info.csv")

    def test_shape(self):
        assert self.df.shape == (4, 13)

    def test_nan(self):
        assert not self.df.isnull().any().any()

    def teardown_class(self):
        Path("video_info.csv").unlink()


class TestExtractFrame():
    def setup_class(self):
        parser([
                "extframe",
                "videos/*.mp4",
                "30"
              ])

    def test_csv(self):
        df = pd.read_csv("frames/frames.csv")
        assert df.shape == (124, 4)

    def test_files1(self):
        p = Path('frames/VS000169').glob('*.jpg')
        assert len(list(p)) == 93

    def test_files2(self):
        p = Path('frames/VS000170').glob('*.jpg')
        assert len(list(p)) == 31

    def teardown_class(self):
        shutil.rmtree("frames")


class TestExtractFov():
    def setup_class(self):
        parser([
                "extfov",
                "videos/VS000169/*.mp4",
                "-s",
                "30,45"
              ])

    def test_csv(self):
        df = pd.read_csv("fovs/fovs.csv")
        assert df.shape == (3, 3)

    def test_files1(self):
        p = Path('fovs/FOV_00-30').glob('*.jpg')
        assert len(list(p)) == 3

    def test_files2(self):
        p = Path('fovs/FOV_00-45').glob('*.jpg')
        assert len(list(p)) == 3

    def teardown_class(self):
        shutil.rmtree("fovs")
