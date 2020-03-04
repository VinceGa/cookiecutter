#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pset_1` package."""

import os
from tempfile import TemporaryDirectory
from unittest import TestCase
import pandas as pd

from pset_1.hash_str import hash_str
from pset_1.io import atomic_write
from pset_1.__main__ import (
    get_user_hash,
    convert_excel_to_parquet,
    read_parquet_columns)


class FakeFileFailure(IOError):
    pass


class HashTests(TestCase):
    def test_basic(self):
        # The result is from https://md5calc.com/hash/sha256/John+Doe%0D%0A
        self.assertEqual(hash_str("John", salt=" Doe").hex()[:6], "ec9a41")
        self.assertEqual(hash_str("world!", salt="hello, ").hex()[:6], "68e656")
    
    def test_empty_string(self):
        # The result is from the above site.
        self.assertEqual(hash_str("",salt="").hex()[:6], "e3b0c4")
    
    def test_invalid_input(self):
        self.assertRaises(TypeError, hash_str,3.14)
        #TODO add more cases


class AtomicWriteTests(TestCase):
    def test_atomic_write(self):
        """Ensure file exists after being written successfully"""

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "abcd.txt")

            with atomic_write(fp, "w") as f:
                assert not os.path.exists(fp)
                tmpfile = f.name
                f.write("1234")

            assert not os.path.exists(tmpfile)
            assert os.path.exists(fp)

            with open(fp) as f:
                self.assertEqual(f.read(), "1234")

    def test_atomic_failure(self):
        """Ensure that file does not exist after failure during write"""

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "abcd.txt")

            with self.assertRaises(FakeFileFailure):
                with atomic_write(fp, "w") as f:
                    tmpfile = f.name
                    assert os.path.exists(tmpfile)
                    raise FakeFileFailure()

            assert not os.path.exists(tmpfile)
            assert not os.path.exists(fp)

    def test_file_exists(self):
        """Ensure an error is raised when a file exists"""
        with TemporaryDirectory() as temp:
            fp=os.path.join(temp, "qwert.txt")

            with atomic_write(fp, "w") as f:
                f.write("qwert")
            
            assert os.path.exists(fp)

            try:
                with atomic_write(fp, "w") as f:
                    f.write("1234")
            except FileExistsError as e:
                self.assertIsInstance(e, FileExistsError)

class ParquetTests(TestCase):
    def test_convert_xlsx_to_parquet(self):
        """ensure xlsx can be converted to parquet file"""
        
        with TemporaryDirectory() as tmp:
            tst_xlsx = os.path.join(tmp, "testfile.xlsx")
            tst_parquet = os.path.join(tmp, "testfile.parquet")

            # create dataframe with some data
            df = pd.DataFrame({"a": [1, 2], "b": ["hello", "world"]})

            # save df to xlsx file
            with atomic_write(tst_xlsx, as_file=False) as f:
                df.to_excel(f)

            parquet_file = convert_excel_to_parquet(tst_xlsx)

            self.assertEqual(tst_parquet, parquet_file)
            self.assertTrue(os.path.exists(tst_parquet))
           
            df_parquet = pd.read_parquet(tst_parquet, engine="pyarrow")
            self.assertTrue(df.equals(df_parquet))

    def test_read_cols(self):
        """ensure the data is unchanged in both formats"""
        with TemporaryDirectory() as temp:
            parquet_file = os.path.join(temp, "testfile.parquet")

            df = pd.DataFrame({"a": [1, 2], "b": ["hello", "world"]})

            # save df to parquet file
            with atomic_write(parquet_file, as_file=False) as f:
                df.to_parquet(f, engine="pyarrow")

            # read specific columns using the read_parquet_columns function
            col_a = read_parquet_columns(parquet_file, ["a"])
            col_b = read_parquet_columns(parquet_file, ["b"])

            # ensure we are getting dataframe instances
            for tmp_df in [col_a, col_b]:
                self.assertIsInstance(tmp_df, pd.DataFrame)

            # ensure content of extracted columns match expected values
            for col, result in zip(
                [["a"], ["b"]], [col_a, col_b]
                ):
                self.assertTrue(df[col].equals(result))
        
