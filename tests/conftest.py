import os
from typing import Iterator

from moto import mock_aws
import pytest


@pytest.fixture(autouse=True, scope="session")
def _aws_credentials() -> None:
    # Always stub these out for testing
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def _aws(_aws_credentials: None) -> Iterator[None]:
    with mock_aws():
        yield
