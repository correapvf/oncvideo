from pathlib import Path
import pytest
import numpy as np
import pandas as pd
from oncvideo.arg_parser import main as parser
from oncvideo._utils import URL, download_file
from oncvideo.didson_file import read_ddf

FILE = 'DIDSON3000SN374_20221108T160900.221Z.ddf'

def finalizer_function():
    Path(FILE).unlink()

@pytest.fixture(scope="session", autouse=True)
def setup_session(request):
    download_file(URL+FILE, Path(FILE))
    request.addfinalizer(finalizer_function)


class TestDidsonInfo():
    def setup_class(self):
        parser([
                "didson",
                FILE
              ])
        self.df = pd.read_csv("DIDSON_info.csv")

    def test_shape(self):
        assert self.df.shape == (1, 11)

    def test_nan(self):
        assert not self.df.isnull().any().any()

    def teardown_class(self):
        Path("DIDSON_info.csv").unlink()


class TestDidsonRead():
    def setup_class(self):
        self.out = read_ddf(FILE)

    def test_ts_shape(self):
        assert len(self.out[0]) == 3447

    def test_ts_nan(self):
        assert not self.out[0].isnull().any()

    def test_array_shape(self):
        assert self.out[1].shape == (3447, 96, 512)

    def test_array_nan(self):
        assert not np.isnan(self.out[1]).any()
