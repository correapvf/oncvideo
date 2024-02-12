from pathlib import Path
import shutil
import pandas as pd
from oncvideo.arg_parser import main as parser

class TestDives():
    def setup_class(self):
        parser([
                "getDives",
                "-t",
                "c1416a5f-2dc7-4cc6-83f0-17a8261f9826",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape_columns(self):
        assert self.df.shape[1] == 12

    def test_shape_line(self):
        assert self.df.shape[0] > 1000

    def test_device_code(self):
        assert self.df['deviceCode'].isnull().sum() < len(self.df)*0.8

    def test_location_code(self):
        assert self.df['locationCode'].isnull().sum() < len(self.df)*0.8

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestDownloadTssd():
    def setup_class(self):
        # need to use exec in this case, or the test will fail for some unkown reason
        exec(open('tests/tssd.py').read())
        self.df = pd.read_csv("output_merged.csv")

    def test_shape(self):
        df = pd.read_csv("output/output.csv")
        assert df.shape == (2, 6)

    def test_shape_merged(self):
        assert self.df.shape == (4, 31)

    def test_nav(self):
        assert not self.df['Longitude (deg)'].isnull().any()

    def test_ctd(self):
        assert not self.df['Pressure (decibar)'].isnull().any()

    def test_file_ctd(self):
        p = Path('output/ROVData_Odysseus_ConductivityTemperatureDepth_20220729T053221Z_20220729T075227Z.csv')
        assert p.is_file()

    def test_file_nav(self):
        p = Path('output/ROVData_Odysseus_NavigationSystem_20220729T053221Z_20220729T075227Z.csv')
        assert p.is_file()

    def teardown_class(self):
        Path("output_merged.csv").unlink()
        shutil.rmtree("output")
