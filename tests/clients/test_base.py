import pytest

from keboola_mcp_server.clients.base import normalize_storage_api_url


class TestNormalizeStorageApiUrl:
    """`normalize_storage_api_url` requires a genuine Keboola stack domain, not just a
    `connection.` prefix -- see the "Security hardening" RFC increment (`connection.attacker.tld`
    previously passed, letting a caller-supplied host receive the live bearer token)."""

    @pytest.mark.parametrize(
        ('url', 'expected'),
        [
            ('https://connection.keboola.com', 'https://connection.keboola.com'),
            ('https://connection.eu-central-1.keboola.com', 'https://connection.eu-central-1.keboola.com'),
            (
                'https://connection.north-europe.azure.keboola.com',
                'https://connection.north-europe.azure.keboola.com',
            ),
            (
                'https://connection.europe-west3.gcp.keboola.com',
                'https://connection.europe-west3.gcp.keboola.com',
            ),
            ('https://connection.canary-orion.keboola.dev', 'https://connection.canary-orion.keboola.dev'),
            ('https://connection.keboola.com:443', 'https://connection.keboola.com'),
            ('https://connection.keboola.com/v2/storage', 'https://connection.keboola.com'),
        ],
    )
    def test_accepts_genuine_keboola_stack_hosts(self, url: str, expected: str) -> None:
        assert normalize_storage_api_url(url) == expected

    @pytest.mark.parametrize(
        'url',
        [
            'https://connection.attacker.tld',
            'https://connection.attacker.example',
            'https://connection.keboola.com.attacker.example',
            'https://connection.keboola.com.example.com',
            'https://connection.example.com',
            'https://sapi.keboola.com',  # no 'connection.' label at all
            'https://keboola.com',
            '',
        ],
    )
    def test_rejects_lookalike_or_foreign_hosts(self, url: str) -> None:
        with pytest.raises(ValueError, match='Invalid Keboola Storage API URL'):
            normalize_storage_api_url(url)
