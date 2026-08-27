import json

import pytest

from app.application.services.ingestion_reader import (
    IngestionReadError,
    read_api_records,
    read_file_records,
)


def test_reads_csv_and_json_files(tmp_path):
    csv_path = tmp_path / "records.csv"
    csv_path.write_text("id,name\n1,A\n2,B\n", encoding="utf-8")
    json_path = tmp_path / "records.json"
    json_path.write_text(json.dumps([{"id": 1}, {"id": 2}]), encoding="utf-8")

    csv_rows, _ = read_file_records(str(csv_path))
    json_rows, _ = read_file_records(str(json_path))

    assert csv_rows == [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]
    assert json_rows == [{"id": 1}, {"id": 2}]


def test_json_file_requires_array_root(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text('{"data": []}', encoding="utf-8")
    with pytest.raises(IngestionReadError, match="root must be an array"):
        read_file_records(str(path))


def test_api_page_pagination_and_records_path(monkeypatch):
    responses = [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}],
    ]
    calls = []

    class Response:
        status_code = 200

        def __init__(self, rows):
            self.rows = rows

        def json(self):
            return {"result": {"records": self.rows}}

    def fake_get(url, params, headers, timeout):
        calls.append(params)
        return Response(responses[len(calls) - 1])

    monkeypatch.setattr("app.application.services.ingestion_reader.requests.get", fake_get)
    rows, _ = read_api_records(
        {
            "url": "https://example.test/records",
            "records_path": "result.records",
            "pagination": {
                "mode": "PAGE",
                "start_page": 1,
                "page_size": 2,
            },
        }
    )

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [call["page"] for call in calls] == ["1", "2"]
