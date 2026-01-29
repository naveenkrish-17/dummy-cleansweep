"""Test the cluster module"""

import numpy as np
import pandas as pd
import pytest
from sklearn.cluster import DBSCAN

from cleansweep._types import ClusterDefinition, DBScanConfig
from cleansweep.chunk.semantic.cluster import (
    _split_dataframe_into_clustered_and_unclustered,
    add_cluster_to_dataframe,
    cluster_question_answer_pairs,
    drop_cluster_columns,
    load_cluster_model,
)
from cleansweep.exceptions import PipelineError


class TestAddClusterToDataframe:
    """Test the add_cluster_to_dataframe function"""

    @pytest.fixture(scope="class")
    def df(self):
        """Fixture for a dataframe"""
        return pd.DataFrame(
            {
                "cluster_a": [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]],
                "cluster_b": [[np.array([0])], [np.array([1])], [np.array([2])]],
                "id": [1, 2, 3],
            }
        )

    @pytest.mark.parametrize(
        "model, config",
        [
            pytest.param(None, None, id="no_dbscan"),
            pytest.param(DBSCAN(0.25, min_samples=2), None, id="dbscan"),
            pytest.param(None, DBScanConfig(), id="dbscan_config"),
        ],
    )
    def test_happy(self, df, model, config):
        """Test that function compoletes successfully with and without DBSCAN"""

        cluster_definition = ClusterDefinition(columns_to_embed=["a"])

        df_result, model_result = add_cluster_to_dataframe(
            df, definition=cluster_definition, model=model, config=config
        )

        assert "cluster_a_filter" in df_result.columns
        assert "cluster_b_filter" not in df_result.columns
        if model is not None:
            assert model_result == model

        if config is not None:
            assert model_result.eps == config.eps
            assert model_result.min_samples == config.min_samples

    def test_errors_in_fit_predict(self, df, mocker):
        """Test that an error is raised when there are errors in fit_predict"""

        mocker.patch.object(DBSCAN, "fit_predict", side_effect=ValueError)

        cluster_definition = ClusterDefinition(columns_to_embed=["a"])
        result = add_cluster_to_dataframe(df, definition=cluster_definition)
        assert result[0]["cluster_a_filter"].unique().tolist() == [-1]


class TestDropClusterColumns:
    """Test the drop_cluster_columns function"""

    @pytest.fixture(scope="function")
    def df(self):
        """Fixture for a dataframe"""
        return pd.DataFrame(
            {
                "cluster_a": [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]],
                "cluster_b": [[np.array([0])], [np.array([1])], [np.array([2])]],
                "cluster_a_filter": [0, 1, 2],
                "cluster_b_filter": [0, 1, 2],
                "vector_id": [0, 1, 2],
                "text_to_embed": ["a", "b", "c"],
                "id": [1, 2, 3],
            }
        )

    def test_drop_cluster_columns(self, df):
        """Test that the function drops the cluster columns"""

        df_result = drop_cluster_columns(
            df,
            [
                ClusterDefinition(columns_to_embed=["a"]),
                ClusterDefinition(columns_to_embed=["b"]),
            ],
        )

        assert list(df_result.columns) == ["id"]

    def test_drop_cluster_columns_no_cluster_columns(self, df):
        """Test that the function does not drop any cluster columns if there are no cluster columns"""

        df_result = drop_cluster_columns(df, None)
        cols = list(df_result.columns)

        assert "cluster_a" in cols
        assert "cluster_b" in cols
        assert "cluster_a_filter" in cols
        assert "cluster_b_filter" in cols
        assert "vector_id" not in cols
        assert "text_to_embed" not in cols


class TestSplitDataframeIntoClusteredAndUnclustered:
    """Test the _split_dataframe_into_clustered_and_unclustered function"""

    @pytest.fixture(scope="function")
    def df(self):
        """Fixture for a dataframe"""
        return pd.DataFrame(
            {
                "cluster_a": [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]],
                "cluster_a_filter": [0, -1, 2],
                "id": [1, 2, 3],
                "question_id": [1, 2, 3],
            }
        )

    @pytest.mark.parametrize(
        "id_column",
        [
            pytest.param("id", id="id"),
            pytest.param("question_id", id="question_id"),
            pytest.param(None, id="no_id"),
        ],
    )
    def test_split_dataframe_into_clustered_and_unclustered(self, df, id_column):
        """Test that the function splits the dataframe into clustered and unclustered"""

        clustered_df, unclustered_df = _split_dataframe_into_clustered_and_unclustered(
            df, [ClusterDefinition(columns_to_embed=["a"])], id_column=id_column
        )

        assert len(clustered_df) == 2
        assert len(unclustered_df) == 1

    def test_invalid_cluster(self, df):
        """Test that an error is raised when the cluster column is not found"""

        with pytest.raises(PipelineError):
            _split_dataframe_into_clustered_and_unclustered(
                df, [ClusterDefinition(columns_to_embed=["b"])]
            )


class TestClusterQuestionAnswerPairs:
    """Test the cluster_question_answer_pairs function"""

    @pytest.fixture(scope="class")
    def df(self):
        """Fixture for a dataframe"""
        return pd.DataFrame(
            {
                "cluster_a": [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]],
                "cluster_b": [[np.array([0])], [np.array([1])], [np.array([2])]],
                "cluster_a_filter": [0, 1, 2],
                "cluster_b_filter": [0, 1, 2],
                "vector_id": [0, 1, 2],
                "text_to_embed": ["a", "b", "c"],
                "id": [1, 2, 3],
                "question_id": [1, 2, 3],
            }
        )

    @pytest.mark.parametrize(
        "definitions, id_column",
        [
            pytest.param(
                [ClusterDefinition(columns_to_embed=["a"])], "id", id="single cluster"
            ),
            pytest.param(
                [ClusterDefinition(columns_to_embed=["a"])],
                None,
                id="single cluster w no id",
            ),
            pytest.param(
                [
                    ClusterDefinition(columns_to_embed=["a"]),
                    ClusterDefinition(columns_to_embed=["b"]),
                ],
                "id",
                id="multiple clusters",
            ),
        ],
    )
    def test_cluster_question_answer_pairs(self, df, definitions, id_column):
        """Test that the function clusters the question-answer pairs"""

        df_result = cluster_question_answer_pairs(df, definitions, id_column=id_column)

        assert "cluster_id" in list(df_result.columns)


class TestLoadClusterModel:
    """Test the load_cluster_model function"""

    @pytest.fixture(scope="class", autouse=True)
    def test_setup(self, class_mocker):
        """Setup for the tests"""

        class_mocker.patch(
            "cleansweep.chunk.semantic.cluster.joblib.load", return_value="model"
        )

        class_mocker.patch(
            "cleansweep.chunk.semantic.cluster.create_new_dbscan", return_value="model"
        )

    @pytest.fixture(scope="function")
    def mock_model_store(self, mocker):
        """Fixture for a mock model store"""

        mock_bucket = mocker.MagicMock()
        mock_blob = mocker.MagicMock()
        mock_bucket.blob = mocker.MagicMock(return_value=mock_blob)

        yield mock_bucket

    def test_load_cluster_model(self, mock_model_store):
        """Test that the function loads the cluster model"""

        model = load_cluster_model(
            ClusterDefinition(columns_to_embed=["a"]), mock_model_store
        )
        assert model == "model"

    def test_load_cluster_model_blob_not_exists(self, mock_model_store):
        """Test that the function loads the cluster model"""

        mock_model_store.blob.return_value.exists.return_value = False

        model = load_cluster_model(
            ClusterDefinition(columns_to_embed=["a"]), mock_model_store
        )
        assert model == "model"

    def test_load_cluster_model_store_is_none(self):
        """Test that the function loads the cluster model"""

        model = load_cluster_model(ClusterDefinition(columns_to_embed=["a"]))
        assert model == "model"
