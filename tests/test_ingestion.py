import pytest
from src.ingestion.data_source import DataSource


def test_synthetic_ingestion():
    source = DataSource(use_synthetic=True)
    data = source.fetch(batch_size=10)
    assert len(data) == 10
    assert "email" in data[0]
    assert "name" in data[0]