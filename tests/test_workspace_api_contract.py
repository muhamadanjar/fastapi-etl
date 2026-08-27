from starlette.requests import Request

from app.interfaces.http.middleware.auth import AuthMiddleware
from app.main import app


def _request(method: str, path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "headers": [],
            "query_string": b"",
            "server": ("test", 80),
            "client": ("test", 1),
            "scheme": "http",
        }
    )


def test_only_mvp_route_families_are_published():
    specification = app.openapi()
    paths = set(specification["paths"])
    protected = {path for path in paths if path.startswith("/api/v1/")}
    assert protected
    assert all(
        path.startswith(
            (
                "/api/v1/overview",
                "/api/v1/sources",
                "/api/v1/datasets",
                "/api/v1/recipes",
                "/api/v1/runs",
            )
        )
        for path in protected
    )
    assert not any("/jobs" in path for path in paths)
    source_list_schema = specification["components"]["schemas"][
        "ListEnvelope_DataSourceRead_"
    ]
    assert "metas" in source_list_schema["required"]
    assert "meta" not in source_list_schema["properties"]


def test_permission_mapping_is_resource_and_action_specific():
    cases = {
        ("GET", "/api/v1/sources"): "etl.sources.read",
        ("PATCH", "/api/v1/sources/00000000-0000-0000-0000-000000000000"): "etl.sources.write",
        ("POST", "/api/v1/sources/x/ingestions/file"): "etl.sources.ingest",
        ("GET", "/api/v1/datasets/x/records"): "etl.datasets.read",
        ("GET", "/api/v1/datasets/x/download"): "etl.datasets.download",
        ("DELETE", "/api/v1/datasets/x"): "etl.datasets.delete",
        ("GET", "/api/v1/recipes"): "etl.recipes.read",
        ("POST", "/api/v1/recipes"): "etl.recipes.write",
        ("POST", "/api/v1/recipes/x/runs"): "etl.recipes.run",
        ("GET", "/api/v1/runs"): "etl.runs.read",
    }
    for (method, path), permission in cases.items():
        assert AuthMiddleware._required_permission(_request(method, path)) == permission


def test_celery_has_only_mvp_tasks_and_no_beat_schedule():
    from app.tasks.celery_app import celery_app

    assert celery_app.conf.include == ["app.tasks.run_tasks"]
    assert not celery_app.conf.beat_schedule
    assert set(celery_app.conf.task_routes) == {"etl.*"}
