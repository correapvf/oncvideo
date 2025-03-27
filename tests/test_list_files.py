from pathlib import Path
import pandas as pd
from oncvideo._arg_parser import main as parser

class TestListDc():
    def setup_class(self):
        parser([
                "list",
                # "-t",
                # "API key",
                "-dc",
                "AXISCAMB8A44F04DEEA",
                "-from",
                "2022-09-01",
                "-to",
                "2022-10-02",
                "-s",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (702, 2)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'gap dateFrom of 0 days 00:10:06'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'AXISCAMB8A44F04DEEA_20221001T231400.000Z.mp4'

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestListDive():
    def setup_class(self):
        parser([
                "list",
                # "-t",
                # "API key",
                "-dive",
                "H1987",
                "-s",
                "--extension",
                "mov",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (47, 2)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'start at 00:01:03.000'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'HERCULESROVINSPACZEUSPLUS_20230716T153638.000Z.mov'

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestListDi():
    def setup_class(self):
        parser([
                "list",
                # "-t",
                # "API key",
                "-di",
                "23543",
                "--quality",
                "5000",
                "-from",
                "2020-09-11T02:00:00",
                "-to",
                "2020-09-12T22:34:44",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (116, 10)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'gap dateFrom of 0 days 07:20:48'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'HERCULESROVINSPACZEUSPLUS_20200912T193232.000Z-5000.mp4'

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestListLc():
    def setup_class(self):
        parser([
                "list",
                # "-t",
                # "API key",
                "-lc",
                "BACAX",
                "-from",
                "2023-09-11T02:00:00",
                "-to",
                "2023-09-12T22:34:44",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (45, 10)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'gap dateFrom of 0 days 00:00:10'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'SUBCRAYFIN21017_20230912T220013.000Z.mp4'

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestBlistDive():
    def setup_class(self):
        parser([
                "blist",
                # "-t",
                # "API key",
                "tests/list_dive.csv",
                "--extension",
                "all",
                "--quality",
                "all",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (60, 12)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'start at 00:06:56.000'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'INSPACMINIZEUS4KCAMODYSSEUS_20220729T085502.000Z-LOW.mp4'

    def test_group(self):
        assert self.df['group'].nunique() == 2
    
    def test_quality(self):
        assert self.df['quality'].nunique() == 4
    
    def test_extension(self):
        assert self.df['filename'].str[-3:].nunique() == 2

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestBlistLc():
    def setup_class(self):
        parser([
                "blist",
                # "-t",
                # "API key",
                "tests/list_lc.csv",
                "--quality",
                "standard",
                "-s",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (11124, 3)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'gap dateFrom of 44 days 08:09:42'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'SUBC1CAMMK5_15548_20191231T230058.000Z.mp4'

    def test_group(self):
        assert self.df['group'].nunique() == 2

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestBlistDi():
    def setup_class(self):
        parser([
                "blist",
                # "-t",
                # "API key",
                "tests/list_di.csv",
                "--quality",
                "5000",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (25, 11)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'gap dateFrom of 0 days 23:38:49'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'HERCULESROVINSPACZEUSPLUS_20210822T215835.000Z-5000.mp4'

    def test_group(self):
        assert self.df['group'].nunique() == 2

    def teardown_class(self):
        Path("tmp.csv").unlink()


class TestBlistDc():
    def setup_class(self):
        parser([
                "blist",
                # "-t",
                # "API key",
                "tests/list_dc.csv",
                "-s",
                "-o",
                "tmp.csv"
              ])
        self.df = pd.read_csv("tmp.csv")

    def test_shape(self):
        assert self.df.shape == (36, 3)

    def test_query(self):
        assert self.df['query_offset'].iloc[0] == 'gap dateFrom of 0 days 00:05:03'

    def test_filename(self):
        assert self.df['filename'].iloc[-1] == 'AXISCAMB8A44F04DEEA_20220912T051010.000Z.mp4'

    def test_group(self):
        assert self.df['group'].nunique() == 2

    def teardown_class(self):
        Path("tmp.csv").unlink()
