"""Provision Grafana's least-privilege Elasticsearch account."""

import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

ELASTICSEARCH_URL = "http://elasticsearch:9200"


def main() -> None:
    admin_password = os.environ["ELASTIC_PASSWORD"]
    reader_user = os.environ["ELASTIC_GRAFANA_USER"]
    reader_password = (
        Path("/run/secrets/elastic_grafana_password")
        .read_text(encoding="utf-8")
        .rstrip("\r\n")
    )
    if not reader_password:
        raise ValueError("elastic_grafana_password must be non-empty")

    put(
        "/_security/role/grafana_reader",
        admin_password,
        {
            "cluster": ["monitor"],
            "indices": [
                {
                    "names": ["guardian-logs"],
                    "privileges": ["read", "view_index_metadata"],
                }
            ],
        },
    )
    put(
        f"/_security/user/{reader_user}",
        admin_password,
        {
            "password": reader_password,
            "roles": ["grafana_reader"],
            "enabled": True,
        },
    )


def put(path: str, password: str, payload: dict[str, object]) -> None:
    request = Request(
        f"{ELASTICSEARCH_URL}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    request.add_header("Authorization", basic_auth("elastic", password))
    with urlopen(request, timeout=30) as response:
        if response.status not in (200, 201):
            raise RuntimeError(f"Elasticsearch provisioning failed: {response.status}")


def basic_auth(username: str, password: str) -> str:
    import base64

    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {credentials}"


if __name__ == "__main__":
    main()
