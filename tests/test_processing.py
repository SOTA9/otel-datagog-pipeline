import pytest
from src.processing.transformer import DataTransformer


def test_transform_cleans_data():
    records = [
        {"id": 1, "name": "  john doe  ", "email": "JOHN@EXAMPLE.COM", "company": "Acme"},
        {"id": 1, "name": "  john doe  ", "email": "JOHN@EXAMPLE.COM", "company": "Acme"},  # duplicate
    ]
    transformer = DataTransformer()
    df = transformer.transform(records)
    assert len(df) == 1
    assert df.iloc[0]["email"] == "john@example.com"
    assert df.iloc[0]["name"] == "John Doe"
    assert "processed_at" in df.columns
