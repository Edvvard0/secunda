from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from payment_service import __version__
from payment_service.tools import export_openapi

if TYPE_CHECKING:
    import httpx
    from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[3]
COMMITTED = ROOT / "docs" / "openapi.json"
PROBLEM_JSON = "application/problem+json"


@pytest.fixture
def schema(app: FastAPI) -> dict[str, Any]:
    return app.openapi()


def test_committed_openapi_document_is_up_to_date() -> None:
    fresh = export_openapi.render_openapi()

    committed = COMMITTED.read_text(encoding="utf-8")

    assert committed == fresh, (
        "docs/openapi.json is stale: run python -m payment_service.tools.export_openapi"
    )


def test_export_is_deterministic() -> None:
    assert export_openapi.render_openapi() == export_openapi.render_openapi()


def test_export_ignores_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_DOCS_ENABLED", "false")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    assert export_openapi.render_openapi() == COMMITTED.read_text(encoding="utf-8")


def test_export_writes_pretty_sorted_json_with_unix_newlines(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "openapi.json"

    exit_code = export_openapi.main([str(target)])

    raw = target.read_bytes()
    assert exit_code == 0
    assert raw.endswith(b"}\n")
    assert b"\r" not in raw
    assert raw.decode("utf-8").startswith('{\n  "components"')
    parsed = json.loads(raw)
    assert json.dumps(parsed, indent=2, sort_keys=True, ensure_ascii=False) + "\n" == raw.decode("utf-8")


def test_export_rejects_extra_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    assert export_openapi.main(["a.json", "b.json"]) == 2
    assert "usage:" in capsys.readouterr().err


def test_document_describes_the_service(schema: dict[str, Any]) -> None:
    assert schema["info"]["title"] == "Payment Service API"
    assert schema["info"]["version"] == __version__
    assert {tag["name"] for tag in schema["tags"]} == {"payments", "health"}
    assert set(schema["paths"]) == {
        "/api/v1/payments",
        "/api/v1/payments/{payment_id}",
        "/health/live",
        "/health/ready",
    }


def test_document_declares_the_api_key_security_scheme(schema: dict[str, Any]) -> None:
    assert schema["components"]["securitySchemes"] == {
        "ApiKey": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "Static API key issued to the client.",
        }
    }


def test_payment_operations_require_the_key_and_health_probes_do_not(schema: dict[str, Any]) -> None:
    paths = schema["paths"]

    assert paths["/api/v1/payments"]["post"]["security"] == [{"ApiKey": []}]
    assert paths["/api/v1/payments/{payment_id}"]["get"]["security"] == [{"ApiKey": []}]
    assert "security" not in paths["/health/live"]["get"]
    assert "security" not in paths["/health/ready"]["get"]
    assert "security" not in schema


def test_post_documents_202_with_its_headers_and_the_error_responses(schema: dict[str, Any]) -> None:
    operation = schema["paths"]["/api/v1/payments"]["post"]
    responses = operation["responses"]

    assert set(responses) == {"202", "400", "401", "413", "422"}
    accepted = responses["202"]
    assert accepted["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/PaymentAcceptedResponse"
    }
    assert set(accepted["headers"]) == {"Location", "Idempotent-Replayed"}
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/CreatePaymentRequest"
    }


def test_post_documents_the_idempotency_key_header(schema: dict[str, Any]) -> None:
    parameters = schema["paths"]["/api/v1/payments"]["post"]["parameters"]

    (header,) = [p for p in parameters if p["in"] == "header"]
    assert header["name"] == "Idempotency-Key"
    assert "Required" in header["description"]


def test_get_documents_its_responses(schema: dict[str, Any]) -> None:
    operation = schema["paths"]["/api/v1/payments/{payment_id}"]["get"]

    assert set(operation["responses"]) == {"200", "401", "404", "422"}
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/PaymentResponse"
    }


def test_readiness_documents_the_503_problem(schema: dict[str, Any]) -> None:
    responses = schema["paths"]["/health/ready"]["get"]["responses"]

    assert set(responses) == {"200", "503"}


def test_every_error_response_is_documented_as_problem_json(schema: dict[str, Any]) -> None:
    documented = 0
    for path_item in schema["paths"].values():
        for operation in path_item.values():
            for status, response in operation["responses"].items():
                if int(status) >= 400:
                    documented += 1
                    assert list(response["content"]) == [PROBLEM_JSON], (operation["operationId"], status)
                    assert response["content"][PROBLEM_JSON]["schema"] == {
                        "$ref": "#/components/schemas/ProblemDetails"
                    }
    assert documented == 4 + 3 + 1  # POST, GET, readiness


def test_the_default_fastapi_validation_schema_is_not_advertised(schema: dict[str, Any]) -> None:
    assert "HTTPValidationError" not in schema["components"]["schemas"]
    assert "ValidationError" not in schema["components"]["schemas"]


def test_operation_ids_are_unique_and_stable(schema: dict[str, Any]) -> None:
    ids = [op["operationId"] for item in schema["paths"].values() for op in item.values()]

    assert sorted(ids) == ["createPayment", "getLiveness", "getPayment", "getReadiness"]


def test_money_is_a_string_in_responses_and_requests_accept_both_forms(schema: dict[str, Any]) -> None:
    schemas = schema["components"]["schemas"]

    assert schemas["PaymentResponse"]["properties"]["amount"]["type"] == "string"
    kinds = {variant["type"] for variant in schemas["CreatePaymentRequest"]["properties"]["amount"]["anyOf"]}
    assert kinds == {"number", "string"}


def test_request_schema_forbids_unknown_fields_and_carries_an_example(schema: dict[str, Any]) -> None:
    request = schema["components"]["schemas"]["CreatePaymentRequest"]

    assert request["additionalProperties"] is False
    assert request["examples"][0]["amount"] == "100.50"
    assert request["required"] == ["amount", "currency", "description", "webhook_url"]


def test_every_model_ships_an_example(schema: dict[str, Any]) -> None:
    for name in ("CreatePaymentRequest", "PaymentAcceptedResponse", "PaymentResponse", "ProblemDetails"):
        assert schema["components"]["schemas"][name]["examples"], name


async def test_openapi_json_is_served_and_identical_to_the_generated_document(
    anonymous_client: httpx.AsyncClient, app: FastAPI
) -> None:
    response = await anonymous_client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json() == app.openapi()


async def test_swagger_ui_and_redoc_are_served(anonymous_client: httpx.AsyncClient) -> None:
    assert (await anonymous_client.get("/docs")).status_code == 200
    assert (await anonymous_client.get("/redoc")).status_code == 200
