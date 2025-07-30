"""Tests for VolumetryCalculator."""

import pandas as pd
from unittest.mock import patch

from src.data_quality.core.volumetry_calculator import VolumetryCalculator


class TestVolumetryCalculator:
    """Test cases for VolumetryCalculator."""

    def test_calculate_volume_metrics_small_dataset(self):
        """Test volume calculation for small dataset."""
        # Arrange
        data = pd.DataFrame(
            {"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [1.1, 2.2, 3.3]}
        )

        # Act
        metrics = VolumetryCalculator.calculate_volume_metrics(data)

        # Assert
        assert metrics["row_count"] == 3
        assert metrics["column_count"] == 3
        assert metrics["data_points"] == 9
        assert metrics["memory_usage_bytes"] > 0
        assert metrics["memory_usage_gb"] >= 0
        assert metrics["memory_usage_tb"] >= 0
        assert metrics["estimated_disk_gb"] >= 0
        assert metrics["estimated_disk_tb"] >= 0
        assert isinstance(metrics["memory_usage_bytes"], int)
        assert isinstance(metrics["memory_usage_gb"], float)

    def test_calculate_volume_metrics_empty_dataset(self):
        """Test volume calculation for empty dataset."""
        # Arrange
        data = pd.DataFrame()

        # Act
        metrics = VolumetryCalculator.calculate_volume_metrics(data)

        # Assert
        assert metrics["row_count"] == 0
        assert metrics["column_count"] == 0
        assert metrics["data_points"] == 0
        assert metrics["memory_usage_bytes"] >= 0

    def test_calculate_volume_metrics_large_dataset(self):
        """Test volume calculation for larger dataset."""
        # Arrange
        data = pd.DataFrame({f"col_{i}": range(1000) for i in range(10)})

        # Act
        metrics = VolumetryCalculator.calculate_volume_metrics(data)

        # Assert
        assert metrics["row_count"] == 1000
        assert metrics["column_count"] == 10
        assert metrics["data_points"] == 10000
        assert metrics["memory_usage_gb"] > 0
        assert metrics["estimated_disk_gb"] > metrics["memory_usage_gb"]
        assert (
            abs(metrics["estimated_disk_gb"] - metrics["memory_usage_gb"] * 2.5)
            < 0.0001
        )

    @patch("rich.console.Console")
    def test_display_volume_info(self, mock_console_class):
        """Test volume info display."""
        # Arrange
        mock_console = mock_console_class.return_value
        volume_metrics = {
            "row_count": 100,
            "column_count": 5,
            "data_points": 500,
            "memory_usage_gb": 0.001,
            "memory_usage_tb": 0.000001,
            "estimated_disk_gb": 0.0025,
            "estimated_disk_tb": 0.0000025,
        }

        # Act
        VolumetryCalculator.display_volume_info(volume_metrics)

        # Assert
        assert mock_console.print.call_count >= 5

        # Check that main metrics were displayed
        calls = mock_console.print.call_args_list
        call_strings = [str(call) for call in calls]

        assert any("Dataset Overview" in call_str for call_str in call_strings)
        assert any("100" in call_str for call_str in call_strings)  # row count
        assert any("5" in call_str for call_str in call_strings)  # column count
        assert any("500" in call_str for call_str in call_strings)  # data points

    def test_get_sampling_info_no_sampling(self):
        """Test sampling info when no sampling is used."""
        # Arrange
        total_rows = 100
        analyzed_rows = 100

        # Act
        sampling_info = VolumetryCalculator.get_sampling_info(total_rows, analyzed_rows)

        # Assert
        assert sampling_info["total_table_rows"] == 100
        assert sampling_info["analyzed_rows"] == 100
        assert sampling_info["is_sampled"] is False
        assert sampling_info["sampling_ratio"] == 1.0

    def test_get_sampling_info_with_sampling(self):
        """Test sampling info when sampling is used."""
        # Arrange
        total_rows = 1000
        analyzed_rows = 100

        # Act
        sampling_info = VolumetryCalculator.get_sampling_info(total_rows, analyzed_rows)

        # Assert
        assert sampling_info["total_table_rows"] == 1000
        assert sampling_info["analyzed_rows"] == 100
        assert sampling_info["is_sampled"] is True
        assert sampling_info["sampling_ratio"] == 0.1

    def test_get_sampling_info_zero_total_rows(self):
        """Test sampling info with zero total rows."""
        # Arrange
        total_rows = 0
        analyzed_rows = 0

        # Act
        sampling_info = VolumetryCalculator.get_sampling_info(total_rows, analyzed_rows)

        # Assert
        assert sampling_info["total_table_rows"] == 0
        assert sampling_info["analyzed_rows"] == 0
        assert sampling_info["is_sampled"] is False
        assert sampling_info["sampling_ratio"] == 1.0
