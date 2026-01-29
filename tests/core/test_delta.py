"""Test suite for delta module."""

from datetime import datetime

import pandas as pd
import pytest
import pytz

from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.enumerations import LoadType


class TestDeltaMerge:
    """Test suite delta_merge function"""

    def test_delta_merge(self):
        """Test delta_merge method"""
        df1 = pd.DataFrame(
            {
                "id": ["1", "2", "3", "4"],
                "title": ["test", "test", "test", "test"],
                "content": ["test", "test", "test", "test"],
                "metadata_expiry": [
                    None,
                    None,
                    None,
                    datetime(2021, 1, 1, tzinfo=pytz.UTC),
                ],
                "md5": ["1", "2", "3", "4"],
                "md5_prev": [None, "2", "4", None],
                "action": ["I", "N", "U", "D"],
            }
        )

        df2 = pd.DataFrame(
            {
                "id": ["5", "6", "7", "8"],
                "title": ["test", "test", "test", "test"],
                "content": ["test", "test", "test", "test"],
                "metadata_expiry": [
                    None,
                    None,
                    None,
                    datetime(2021, 1, 1, tzinfo=pytz.UTC),
                ],
                "md5": ["1", "2", "3", "4"],
                "md5_prev": [None, "2", "4", None],
                "action": ["I", "N", "U", "D"],
            }
        )

        df = delta_merge(df1, df2)

        assert len(df.index) == 8

    def test_column_missmatch(self):
        """Test that delta_merge removes additional columns from previous_documents and
        adds columns from processed_documents that don't exist in previous_documents
        """

        df1 = pd.DataFrame(
            {
                "id": ["1", "2", "3", "4"],
                "title": ["test", "test", "test", "test"],
                "content": ["test", "test", "test", "test"],
                "metadata_expiry": [
                    None,
                    None,
                    None,
                    datetime(2021, 1, 1, tzinfo=pytz.UTC),
                ],
                "md5": ["1", "2", "3", "4"],
                "md5_prev": [None, "2", "4", None],
                "action": ["I", "N", "U", "D"],
            }
        )

        df2 = pd.DataFrame(
            {
                "id": ["5", "6", "7", "8"],
                "title": ["test", "test", "test", "test"],
                "content": ["test", "test", "test", "test"],
                "metadata_expiry": [
                    None,
                    None,
                    None,
                    datetime(2021, 1, 1, tzinfo=pytz.UTC),
                ],
                "md5_prev": [None, "2", "4", None],
                "action": ["I", "N", "U", "D"],
                "new_column": ["test", "test", "test", "test"],
            }
        )

        df = delta_merge(df1, df2)

        assert "new_column" not in df.columns
        assert len(df.index) == 8


DELTA_PREPARE_SCENARIOS = [
    pytest.param(
        pd.DataFrame(
            {
                "id": ["2", "3", "5"],
                "title": ["changed", "changed", "test"],
                "content": ["test", "test", "test"],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": ["21", "31", "5"],
                "action": ["U", "N", "I"],
                "metadata_md5": ["1", "2", "3"],
                "metadata_is_modified": ["N", "N", "I"],
            }
        ),
        None,
        LoadType.FULL,
        None,
        None,
        id="full",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": ["2", "5"],
                "title": ["changed", "test"],
                "content": ["test", "test"],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": ["21", "5"],
                "action": ["U", "I"],
                "metadata_md5": ["1", "3"],
                "metadata_is_modified": ["N", "I"],
            }
        ),
        pd.DataFrame(
            {
                "id": ["1", "3", "4"],
                "title": ["test", "test", "test"],
                "content": ["test", "test", "test"],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": ["1", "3", "4"],
                "metadata_md5": ["1", "3", "4"],
                "action": ["N", "N", "N"],
                "metadata_is_modified": ["N", "N", "N"],
            }
        ),
        LoadType.INCREMENTAL,
        None,
        None,
        id="incremental",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": ["2", "5"],
                "title": ["changed", "test"],
                "content": ["test", "test"],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": ["21", "5"],
                "action": ["U", "I"],
                "metadata_md5": ["1", "3"],
                "metadata_is_modified": ["N", "I"],
            }
        ),
        pd.DataFrame(
            {
                "id": ["3"],
                "title": [
                    "test",
                ],
                "content": [
                    "test",
                ],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": ["3"],
                "metadata_md5": ["3"],
                "action": [
                    "N",
                ],
                "metadata_is_modified": [
                    "N",
                ],
            }
        ),
        LoadType.DELTA,
        None,
        None,
        id="delta",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": [
                    "5",
                ],
                "title": [
                    "test",
                ],
                "content": [
                    "test",
                ],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": [
                    "5",
                ],
                "action": [
                    "I",
                ],
                "metadata_md5": [
                    "3",
                ],
                "metadata_is_modified": [
                    "I",
                ],
            }
        ),
        pd.DataFrame(
            {
                "id": [
                    "2",
                    "3",
                ],
                "title": [
                    "test",
                    "test",
                ],
                "content": [
                    "test",
                    "test",
                ],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": [
                    "2",
                    "3",
                ],
                "action": [
                    "I",
                    "N",
                ],
                "metadata_md5": [
                    "2",
                    "3",
                ],
                "metadata_is_modified": [
                    "N",
                    "N",
                ],
            }
        ),
        LoadType.DELTA,
        "metadata",
        None,
        id="filter",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": [
                    "2",
                    "5",
                    "3",
                ],
                "title": [
                    "changed",
                    "test",
                    "test",
                ],
                "content": [
                    "test",
                    "test",
                    "test",
                ],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": [
                    "21",
                    "5",
                    "3",
                ],
                "action": [
                    "U",
                    "I",
                    "U",
                ],
                "metadata_is_modified": [
                    "N",
                    "I",
                    "N",
                ],
                "metadata_md5": [
                    "1",
                    "3",
                    "3",
                ],
            }
        ),
        None,
        LoadType.DELTA,
        None,
        True,
        id="force",
    ),
]


class TestDeltaPrepare:
    """Test suite delta_prepare function"""

    @pytest.fixture(scope="class", autouse=True)
    def mock_get_latest_blob(self, class_mocker):

        mock_blob = class_mocker.MagicMock()
        mock_blob.name = "test.parquet"
        class_mocker.patch(
            "cleansweep.core.delta.get_latest_blob", return_value=mock_blob
        )

        prev_df = pd.DataFrame(
            {
                "id": [
                    "1",
                    "2",
                    "3",
                    "4",
                ],
                "title": [
                    "test",
                    "test",
                    "test",
                    "test",
                ],
                "content": [
                    "test",
                    "test",
                    "test",
                    "test",
                ],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": [
                    "1",
                    "2",
                    "3",
                    "4",
                ],
                "metadata_md5": [
                    "1",
                    "2",
                    "3",
                    "4",
                ],
                "action": [
                    "I",
                    "I",
                    "I",
                    "I",
                ],
                "metadata_is_modified": [
                    "N",
                    "N",
                    "N",
                    "N",
                ],
            }
        )
        class_mocker.patch(
            "cleansweep.core.delta.read_curated_file_to_dataframe",
            return_value=prev_df,
        )

    @pytest.fixture(scope="class")
    def new_df(self):
        yield pd.DataFrame(
            {
                "id": ["2", "3", "5"],
                "title": ["changed", "changed", "test"],
                "content": ["test", "test", "test"],
                "metadata_expiry": [
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                    datetime(2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc),
                ],
                "md5": ["21", "31", "5"],
                "action": ["U", "N", "I"],
                "metadata_md5": ["1", "2", "3"],
                "metadata_is_modified": ["N", "N", "I"],
            }
        )

    @pytest.mark.parametrize(
        "expected_new_df, expected_prev_df, load_type, filter, force",
        DELTA_PREPARE_SCENARIOS,
    )
    def test_func(
        self,
        new_df,
        expected_new_df,
        expected_prev_df,
        load_type,
        filter,
        force,
    ):
        """Test delta_prepare method with different load types"""

        filtered_documents, previous_documents = delta_prepare(
            new_df, load_type, "test_bucket", "*.parquet", filter=filter, force=force
        )

        assert (
            filtered_documents.reset_index(drop=True).to_dict()
            == expected_new_df.reset_index(drop=True).to_dict()
        )

        if expected_prev_df is not None:
            assert (
                previous_documents.reset_index(drop=True).to_dict()
                == expected_prev_df.reset_index(drop=True).to_dict()
            )
        else:
            assert previous_documents is None


class TestDeltaPrepareNoFile:

    def test_no_previous_file(self, mocker):
        """Test that no previous file returns None"""
        mocker.patch(
            "cleansweep.core.delta.get_latest_blob", side_effect=FileNotFoundError
        )
        _, prev = delta_prepare(
            pd.DataFrame(
                {
                    "id": ["1", "2", "3", "4"],
                    "title": ["test", "test", "test", "test"],
                    "content": ["test", "test", "test", "test"],
                    "metadata_expiry": [
                        None,
                        None,
                        None,
                        datetime(2021, 1, 1, tzinfo=pytz.UTC),
                    ],
                    "md5": ["1", "2", "3", "4"],
                    "action": ["I", "I", "I", "I"],
                }
            ),
            LoadType.INCREMENTAL,
            "test_bucket",
            match_glob="*.parquet",
        )

        assert prev is None or prev.empty
