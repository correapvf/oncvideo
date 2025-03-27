from pathlib import Path
import shutil
import pandas as pd
from oncvideo._arg_parser import main as parser

class TestDives():
    def setup_class(self):
        parser([
                "getDives",
                # "-t",
                # "API key",
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
        parser([
        "downloadTS",
        "tests/videos_test.csv",
        "NAV,CTD",
        # "-t",
        # "API key",
        "-p",
        "rov"
        ])
        parser([
                "mergeTS",
                "tests/videos_test.csv",
                "output",
              ])
        self.df = pd.read_csv("merged.csv")

    def test_shape(self):
        df = pd.read_csv("output/output.csv")
        assert df.shape == (4, 6)

    def test_shape_merged(self):
        assert self.df.shape == (4, 29)

    def test_nav(self):
        assert not self.df['longitude_deg'].isnull().any()

    def test_ctd(self):
        assert not self.df['pressure_decibar'].isnull().any()

    def test_file_ctd(self):
        p = Path('output/SBECTD19p7875_ODYSS_CTD_20220729T053221.000Z_20220729T065225.000Z.csv') # ROVData_Odysseus_ConductivityTemperatureDepth_20220729T053221Z_20220729T075227Z.csv
        assert p.is_file()

    def test_file_nav(self):
        p = Path('output/ODYSSEUSROVNAV01_ODYSS_NAV_20220729T053221.000Z_20220729T065225.000Z.csv') # ROVData_Odysseus_NavigationSystem_20220729T053221Z_20220729T075227Z.csv
        assert p.is_file()

    def teardown_class(self):
        Path("merged.csv").unlink()
        shutil.rmtree("output")
