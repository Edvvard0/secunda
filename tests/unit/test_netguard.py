from __future__ import annotations

import socket

import pytest

from payment_service.common.netguard import (
    MAX_URL_LENGTH,
    UnsafeTargetError,
    is_allowed_ip,
    is_public_ip,
    parse_webhook_url,
    resolve_public_addresses,
    validate_webhook_url,
)


def strict(url: str) -> str:
    return validate_webhook_url(url, allow_private_networks=False)


def relaxed(url: str) -> str:
    return validate_webhook_url(url, allow_private_networks=True)


# --------------------------------------------------------------------------- #
# validate_webhook_url - accepted
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("url", "normalised"),
    [
        ("https://example.com/hook", "https://example.com/hook"),
        ("http://hooks.example.co.uk:8080/a/b?x=1&y=2", "http://hooks.example.co.uk:8080/a/b?x=1&y=2"),
        ("HTTPS://Example.COM/Path", "https://example.com/Path"),
        ("https://example.com", "https://example.com/"),
        ("https://example.com:443/x", "https://example.com/x"),
        ("http://example.com:80/x", "http://example.com/x"),
        ("https://example.com/x#fragment", "https://example.com/x"),
        ("https://example.com./x", "https://example.com/x"),
        ("https://sub.domain.example.org:65535/", "https://sub.domain.example.org:65535/"),
        ("https://93.184.216.34/x", "https://93.184.216.34/x"),
        ("https://[2606:4700:4700::1111]:8443/x", "https://[2606:4700:4700::1111]:8443/x"),
        ("https://xn--e1afmkfd.xn--p1ai/", "https://xn--e1afmkfd.xn--p1ai/"),
        ("https://пример.рф/путь?q=ж", "https://xn--e1afmkfd.xn--p1ai/%D0%BF%D1%83%D1%82%D1%8C?q=%D0%B6"),
        ("https://example.com/a%20b?c=d%26e", "https://example.com/a%20b?c=d%26e"),
        ("https://1.example.com/", "https://1.example.com/"),
    ],
)
def test_validate_webhook_url_accepts_public_targets_and_normalises(url: str, normalised: str) -> None:
    assert strict(url) == normalised


def test_validate_webhook_url_accepts_url_of_exactly_the_maximum_length() -> None:
    prefix = "https://example.com/"
    url = prefix + "a" * (MAX_URL_LENGTH - len(prefix))

    assert strict(url) == url


# --------------------------------------------------------------------------- #
# validate_webhook_url - rejected
# --------------------------------------------------------------------------- #
PRIVATE_AND_SPECIAL_ADDRESSES = [
    "http://127.0.0.1/",
    "http://127.0.0.1:8080/hook",
    "http://127.255.255.254/",
    "http://10.0.0.1/",
    "http://10.255.255.255/",
    "http://172.16.0.1/",
    "http://172.31.255.255/",
    "http://192.168.0.1/",
    "http://192.168.255.255/",
    "http://169.254.169.254/latest/meta-data/",
    "http://169.254.0.1/",
    "http://0.0.0.0/",
    "http://100.64.0.1/",
    "http://100.127.255.255/",
    "http://192.0.0.8/",
    "http://198.18.0.1/",
    "http://224.0.0.1/",
    "http://239.255.255.250/",
    "http://240.0.0.1/",
    "http://255.255.255.255/",
    "http://[::1]/",
    "http://[::]/",
    "http://[fe80::1]/",
    "http://[fc00::1]/",
    "http://[fd12:3456::1]/",
    "http://[ff02::1]/",
    "http://[2001:db8::1]/",
    # IPv4 smuggled inside IPv6
    "http://[::ffff:127.0.0.1]/",
    "http://[::ffff:7f00:1]/",
    "http://[::ffff:10.0.0.1]/",
    "http://[::ffff:169.254.169.254]/",
    "http://[::127.0.0.1]/",
    "http://[64:ff9b::7f00:1]/",
    "http://[2002:7f00:1::]/",
]


@pytest.mark.parametrize("url", PRIVATE_AND_SPECIAL_ADDRESSES)
def test_validate_webhook_url_rejects_non_public_ip_literals(url: str) -> None:
    with pytest.raises(UnsafeTargetError, match="not publicly routable"):
        strict(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://2130706433/",  # decimal 127.0.0.1
        "http://0x7f000001/",  # hex 127.0.0.1
        "http://0x7f.1/",
        "http://0x7f.0x0.0x0.0x1/",
        "http://0177.0.0.1/",  # octal octet
        "http://017700000001/",  # octal 127.0.0.1
        "http://127.1/",  # shorthand
        "http://127.0.1/",
        "http://010.0.0.1/",  # leading zero: octal in inet_aton
        "http://1.2.3.4.5/",
        "http://0/",
        "http://4294967295/",
        "http://example.0x7f/",
        "http://example.123/",
    ],
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_ambiguous_numeric_hosts_in_any_mode(
    url: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError, match="ambiguous numeric host"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    ("spelling", "expected"),
    [
        ("2130706433", "127.0.0.1"),
        ("0x7f.1", "127.0.0.1"),
        ("127.1", "127.0.0.1"),
        ("0177.0.0.1", "127.0.0.1"),
    ],
)
def test_ambiguous_spellings_really_are_what_inet_aton_resolves(spelling: str, expected: str) -> None:
    """Documents why they are refused: the OS resolver turns them into internal addresses."""
    assert socket.inet_ntoa(socket.inet_aton(spelling)) == expected


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/",
        "http://LOCALHOST:8080/",
        "http://localhost./",
        "http://api.localhost/",
        "http://printer.local/",
        "http://metadata.internal/",
        "http://db.internal./",
        "http://host.localdomain/",
        "http://router.home.arpa/",
        "http://nas.lan/",
    ],
)
def test_validate_webhook_url_rejects_internal_host_names(url: str) -> None:
    with pytest.raises(UnsafeTargetError, match="local or internal"):
        strict(url)


@pytest.mark.parametrize("url", ["http://redis/", "http://webhook-sink:9000/ok", "http://rabbitmq:15672/"])
def test_validate_webhook_url_rejects_single_label_host_names(url: str) -> None:
    with pytest.raises(UnsafeTargetError, match="fully qualified"):
        strict(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://user:pass@example.com/",
        "http://user@example.com/",
        "http://:pass@example.com/",
        "https://admin:secret@93.184.216.34/",
        "http://127.0.0.1:80@example.com/",
    ],
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_credentials_in_any_mode(url: str, allow_private_networks: bool) -> None:
    with pytest.raises(UnsafeTargetError, match="credentials"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


def test_validate_webhook_url_error_never_echoes_credentials() -> None:
    with pytest.raises(UnsafeTargetError) as excinfo:
        strict("https://admin:hunter2@example.com/?token=abc")

    assert "hunter2" not in str(excinfo.value)
    assert "token" not in str(excinfo.value)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "file:///etc/passwd",
        "gopher://example.com/",
        "javascript:alert(1)",
        "ws://example.com/",
        "//example.com/path",
        "example.com/path",
    ],
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_other_schemes_in_any_mode(
    url: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError, match="scheme"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    "url", ["http://", "https:///path", "http:example.com", "http://:8080/", "http://?x=1"]
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_missing_host_in_any_mode(
    url: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError, match="no host"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    "url", ["http://example.com:0/", "http://example.com:70000/", "http://example.com:65536/"]
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_ports_out_of_range(url: str, allow_private_networks: bool) -> None:
    with pytest.raises(UnsafeTargetError, match="port"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    "url", ["http://example.com:/", "http://example.com:abc/", "http://example.com:80:90/"]
)
def test_validate_webhook_url_rejects_malformed_ports(url: str) -> None:
    with pytest.raises(UnsafeTargetError):
        strict(url)


@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_over_long_urls_in_any_mode(allow_private_networks: bool) -> None:
    url = "https://example.com/" + "a" * MAX_URL_LENGTH

    with pytest.raises(UnsafeTargetError, match="longer than"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize("url", ["", "   "])
def test_validate_webhook_url_rejects_blank_input(url: str) -> None:
    with pytest.raises(UnsafeTargetError):
        strict(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com\\@127.0.0.1/",  # a backslash is a slash to browsers and "@" starts userinfo
        "http://example.com/a b",
        "http://example.com/\n",
        "http://exa\tmple.com/",
        "http://example.com/\x00",
        "http://example.com/\x7f",
        " http://example.com/",
        "http://example.com/ ",
    ],
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_rejects_whitespace_control_characters_and_backslashes(
    url: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError, match="whitespace"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    "url",
    [
        "http://ex%61mple.com/",
        "http://127.0.0.%31/",
        "http://[fe80::1%25eth0]/",
        "http://[fe80::1%eth0]/",
        "http://exa_mple.com/",
        "http://-example.com/",
        "http://example-.com/",
        "http://exam!ple.com/",
        "http://a..b.com/",
        "http://[::1/",
        "http://[example.com]/",
        "http://[1.2.3.4]/",
        "http://" + "a" * 64 + ".example.com/",
        "http://" + ".".join(["a" * 60] * 5) + "/",
    ],
)
def test_validate_webhook_url_rejects_malformed_hosts(url: str) -> None:
    with pytest.raises(UnsafeTargetError):
        strict(url)


def test_validate_webhook_url_normalises_unicode_digits_before_judging_the_host() -> None:
    # Full-width digits and dots are IDNA-mapped to "127.0.0.1" by browsers and resolvers.
    with pytest.raises(UnsafeTargetError, match="not publicly routable"):
        strict("http://１２７．０．０．１/")


# --------------------------------------------------------------------------- #
# validate_webhook_url - allow_private_networks=True relaxes only the network checks
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("url", "normalised"),
    [
        ("http://localhost:9000/ok", "http://localhost:9000/ok"),
        ("http://127.0.0.1/", "http://127.0.0.1/"),
        ("http://127.0.0.1:8080/hook", "http://127.0.0.1:8080/hook"),
        ("http://10.1.2.3:9000/ok", "http://10.1.2.3:9000/ok"),
        ("http://172.20.0.5/", "http://172.20.0.5/"),
        ("http://192.168.1.10/", "http://192.168.1.10/"),
        ("http://[::1]:8000/x", "http://[::1]:8000/x"),
        ("http://webhook-sink:9000/ok", "http://webhook-sink:9000/ok"),
        ("http://webhook_sink:9000/ok", "http://webhook_sink:9000/ok"),
        ("http://printer.local/", "http://printer.local/"),
        ("http://db.internal/", "http://db.internal/"),
        ("https://example.com/", "https://example.com/"),
    ],
)
def test_validate_webhook_url_with_allow_private_networks_accepts_private_targets(
    url: str, normalised: str
) -> None:
    assert relaxed(url) == normalised


def test_validate_webhook_url_strict_mode_rejects_underscore_in_host_names() -> None:
    with pytest.raises(UnsafeTargetError):
        strict("http://webhook_sink.example.com/")


# --------------------------------------------------------------------------- #
# address policy - what allow_private_networks=True relaxes and what it never does
# --------------------------------------------------------------------------- #
NEVER_ALLOWED_ADDRESSES = [
    # IPv4 link-local: cloud metadata (AWS, GCP, Azure, DigitalOcean ...) and ECS task metadata
    "169.254.169.254",
    "169.254.170.2",
    "169.254.0.1",
    "169.254.255.255",
    # CGNAT, including the Alibaba Cloud metadata address
    "100.64.0.1",
    "100.100.100.200",
    "100.127.255.255",
    # unspecified / "this network"
    "0.0.0.0",  # noqa: S104 - an address under test, not a bind
    "0.1.2.3",
    "::",
    # multicast, reserved, broadcast
    "224.0.0.1",
    "239.255.255.250",
    "240.0.0.1",
    "255.255.255.255",
    "ff02::1",
    "ff05::2",
    # IPv6 link-local and the AWS IPv6 metadata address (inside the otherwise allowed fc00::/7)
    "fe80::1",
    "febf::1",
    "fd00:ec2::254",
    "fd00:0ec2:0000:0000:0000:0000:0000:0254",
    # Azure WireServer
    "168.63.129.16",
    # IPv4-mapped / embedded forms of forbidden IPv4 addresses
    "::ffff:169.254.169.254",
    "::ffff:a9fe:a9fe",
    "::ffff:100.100.100.200",
    "::ffff:0.0.0.0",
    "::169.254.169.254",
    "64:ff9b::a9fe:a9fe",
    "2002:a9fe:a9fe::",
    # IPv4-mapped addresses are never accepted, not even for otherwise allowed ranges
    "::ffff:127.0.0.1",
    "::ffff:10.0.0.1",
]

PRIVATE_USE_ADDRESSES = [
    "127.0.0.1",
    "127.255.255.254",
    "::1",
    "10.0.0.1",
    "10.255.255.255",
    "172.16.0.1",
    "172.31.255.255",
    "192.168.0.1",
    "192.168.255.255",
    "fc00::1",
    "fd12:3456:789a::1",
    "fd00:ec2::255",  # next to the metadata address, but not it
    "fd00:ec3::254",
]

NOT_PRIVATE_USE_NOR_PUBLIC_ADDRESSES = [
    "192.0.2.1",  # documentation
    "198.51.100.1",
    "203.0.113.9",
    "198.18.0.1",  # benchmarking
    "192.0.0.8",  # IETF protocol assignments
    "2001:db8::1",
    "fec0::1",  # deprecated site-local
    "2002:7f00:1::",  # 6to4 of 127.0.0.1
    "64:ff9b::7f00:1",  # NAT64 of 127.0.0.1
]


def bracket(address: str) -> str:
    return f"[{address}]" if ":" in address else address


@pytest.mark.parametrize("address", NEVER_ALLOWED_ADDRESSES)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_never_allows_dangerous_ip_literals_whatever_the_flag(
    address: str, allow_private_networks: bool
) -> None:
    url = f"http://{bracket(address)}/latest/meta-data"

    with pytest.raises(UnsafeTargetError):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize("address", NEVER_ALLOWED_ADDRESSES)
def test_error_for_a_dangerous_ip_literal_with_private_networks_allowed_says_so(address: str) -> None:
    with pytest.raises(UnsafeTargetError, match="never allowed"):
        relaxed(f"http://{bracket(address)}/")


@pytest.mark.parametrize("address", PRIVATE_USE_ADDRESSES)
def test_validate_webhook_url_with_allow_private_networks_accepts_private_use_ip_literals(
    address: str,
) -> None:
    assert relaxed(f"http://{bracket(address)}:9000/ok") == f"http://{bracket(address)}:9000/ok"


@pytest.mark.parametrize("address", PRIVATE_USE_ADDRESSES)
def test_validate_webhook_url_without_allow_private_networks_still_rejects_private_use_ip_literals(
    address: str,
) -> None:
    with pytest.raises(UnsafeTargetError, match="not publicly routable"):
        strict(f"http://{bracket(address)}/")


@pytest.mark.parametrize("address", NOT_PRIVATE_USE_NOR_PUBLIC_ADDRESSES)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_allow_private_networks_is_an_allow_list_not_a_switch_that_disables_the_checks(
    address: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError):
        validate_webhook_url(f"http://{bracket(address)}/", allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    "url",
    [
        "http://metadata/computeMetadata/v1/",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://metadata.goog/",
        "http://METADATA.GOOGLE.INTERNAL/",
        "http://Metadata.Google.Internal./",
        "http://metadata.goog./",
        "http://Metadata/",
        "https://metadata.google.internal:8443/x?y=1",
    ],
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_never_allows_metadata_host_names_whatever_the_flag(
    url: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize(
    "url",
    [
        "http://metadata.example.com/",  # not the metadata service names
        "http://metadata2/",
        "http://my-metadata.internal/",
        "http://goog/",
        "http://notmetadata.goog.example.org/",
    ],
)
def test_validate_webhook_url_with_allow_private_networks_accepts_names_that_only_resemble_metadata(
    url: str,
) -> None:
    relaxed(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://2852039166/",  # decimal 169.254.169.254
        "http://0xa9fea9fe/",  # hex 169.254.169.254
        "http://0251.0376.0251.0376/",  # octal 169.254.169.254
        "http://169.254.43518/",
        "http://0xa9.0xfe.0xa9.0xfe/",
        "http://1717986918/",
    ],
)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_validate_webhook_url_never_allows_obfuscated_metadata_addresses(
    url: str, allow_private_networks: bool
) -> None:
    with pytest.raises(UnsafeTargetError, match="ambiguous numeric host"):
        validate_webhook_url(url, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize("address", PRIVATE_USE_ADDRESSES)
def test_is_allowed_ip_accepts_private_use_addresses_only_with_the_flag(address: str) -> None:
    assert is_allowed_ip(address, allow_private_networks=True)
    assert not is_allowed_ip(address, allow_private_networks=False)
    assert not is_public_ip(address)


@pytest.mark.parametrize("address", NEVER_ALLOWED_ADDRESSES + NOT_PRIVATE_USE_NOR_PUBLIC_ADDRESSES)
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_is_allowed_ip_rejects_dangerous_addresses_whatever_the_flag(
    address: str, allow_private_networks: bool
) -> None:
    assert not is_allowed_ip(address, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize("address", ["93.184.216.34", "8.8.8.8", "2606:4700:4700::1111"])
@pytest.mark.parametrize("allow_private_networks", [False, True])
def test_is_allowed_ip_accepts_public_addresses_whatever_the_flag(
    address: str, allow_private_networks: bool
) -> None:
    assert is_allowed_ip(address, allow_private_networks=allow_private_networks)


@pytest.mark.parametrize("address", ["not-an-ip", "", "999.1.1.1", "2130706433"])
def test_is_allowed_ip_rejects_unparsable_input_with_the_flag_on(address: str) -> None:
    assert not is_allowed_ip(address, allow_private_networks=True)


# --------------------------------------------------------------------------- #
# parse_webhook_url
# --------------------------------------------------------------------------- #
def test_parse_webhook_url_returns_normalised_parts() -> None:
    target = parse_webhook_url("HTTPS://Hooks.Example.com:8443/a/b?x=1#f", allow_private_networks=False)

    assert target.scheme == "https"
    assert target.host == "hooks.example.com"
    assert target.port == 8443
    assert target.request_target == "/a/b?x=1"
    assert target.host_header == "hooks.example.com:8443"
    assert target.url == "https://hooks.example.com:8443/a/b?x=1"


def test_parse_webhook_url_uses_scheme_default_port_and_omits_it_from_host_header() -> None:
    https = parse_webhook_url("https://example.com/x", allow_private_networks=False)
    http = parse_webhook_url("http://example.com/x", allow_private_networks=False)

    assert (https.port, https.host_header) == (443, "example.com")
    assert (http.port, http.host_header) == (80, "example.com")


def test_parse_webhook_url_brackets_ipv6_in_host_header() -> None:
    target = parse_webhook_url("http://[2606:4700:4700::1111]:8080/", allow_private_networks=False)

    assert target.host == "2606:4700:4700::1111"
    assert target.host_header == "[2606:4700:4700::1111]:8080"


def test_parse_webhook_url_gives_empty_path_a_slash() -> None:
    target = parse_webhook_url("https://example.com?x=1", allow_private_networks=False)

    assert target.request_target == "/?x=1"


# --------------------------------------------------------------------------- #
# is_public_ip
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "ip",
    ["93.184.216.34", "8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "2a00:1450:4001:81b::200e"],
)
def test_is_public_ip_accepts_global_unicast_addresses(ip: str) -> None:
    assert is_public_ip(ip)


@pytest.mark.parametrize(
    "ip",
    [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "0.0.0.0",  # noqa: S104 - an address under test, not a bind
        "100.64.0.1",
        "224.0.0.1",
        "240.0.0.1",
        "255.255.255.255",
        "::1",
        "::",
        "fe80::1",
        "fc00::1",
        "ff02::1",
        "::ffff:127.0.0.1",
        "::ffff:8.8.8.8",
        "::7f00:1",
        "64:ff9b::7f00:1",
        "64:ff9b::808:808",
        "2002:7f00:1::",
        "2002:0a00:0001::",
        "not-an-ip",
        "",
        "999.1.1.1",
        "2130706433",
    ],
)
def test_is_public_ip_rejects_everything_else(ip: str) -> None:
    assert not is_public_ip(ip)


# --------------------------------------------------------------------------- #
# resolve_public_addresses
# --------------------------------------------------------------------------- #
class FakeResolver:
    def __init__(self, answer: list[str] | Exception) -> None:
        self.answer = answer
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, host: str, port: int, /) -> list[str]:
        self.calls.append((host, port))
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


async def test_resolve_public_addresses_returns_all_public_addresses_in_order() -> None:
    resolver = FakeResolver(["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"])

    addresses = await resolve_public_addresses(
        "example.com", 443, allow_private_networks=False, resolver=resolver
    )

    assert addresses == ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"]
    assert resolver.calls == [("example.com", 443)]


async def test_resolve_public_addresses_removes_duplicates() -> None:
    resolver = FakeResolver(["93.184.216.34", "93.184.216.34", "8.8.8.8", "93.184.216.34"])

    addresses = await resolve_public_addresses(
        "example.com", 80, allow_private_networks=False, resolver=resolver
    )

    assert addresses == ["93.184.216.34", "8.8.8.8"]


@pytest.mark.parametrize(
    "private_address",
    ["10.0.0.5", "127.0.0.1", "169.254.169.254", "192.168.1.1", "::1", "::ffff:10.0.0.1", "100.64.0.1"],
)
async def test_resolve_public_addresses_rejects_answer_mixing_public_and_private_records(
    private_address: str,
) -> None:
    resolver = FakeResolver(["93.184.216.34", private_address])

    with pytest.raises(UnsafeTargetError, match="non-public"):
        await resolve_public_addresses(
            "evil.example.com", 443, allow_private_networks=False, resolver=resolver
        )


async def test_resolve_public_addresses_rejects_private_record_listed_first() -> None:
    resolver = FakeResolver(["10.0.0.5", "93.184.216.34"])

    with pytest.raises(UnsafeTargetError, match="non-public"):
        await resolve_public_addresses(
            "evil.example.com", 443, allow_private_networks=False, resolver=resolver
        )


async def test_resolve_public_addresses_rejects_empty_answer() -> None:
    resolver = FakeResolver([])

    with pytest.raises(UnsafeTargetError, match="did not resolve"):
        await resolve_public_addresses("example.com", 443, allow_private_networks=False, resolver=resolver)


@pytest.mark.parametrize(
    "error",
    [socket.gaierror(socket.EAI_NONAME, "Name or service not known"), TimeoutError(), OSError("boom")],
)
async def test_resolve_public_addresses_rejects_resolution_errors(error: OSError) -> None:
    resolver = FakeResolver(error)

    with pytest.raises(UnsafeTargetError, match="cannot resolve") as excinfo:
        await resolve_public_addresses("example.com", 443, allow_private_networks=False, resolver=resolver)

    assert excinfo.value.__cause__ is error


async def test_resolve_public_addresses_rejects_answers_that_are_not_ip_addresses() -> None:
    resolver = FakeResolver(["93.184.216.34", "not-an-ip"])

    with pytest.raises(UnsafeTargetError, match="invalid address"):
        await resolve_public_addresses("example.com", 443, allow_private_networks=True, resolver=resolver)


async def test_resolve_public_addresses_allows_private_addresses_when_flag_is_on() -> None:
    resolver = FakeResolver(["10.0.0.5", "127.0.0.1", "93.184.216.34"])

    addresses = await resolve_public_addresses(
        "sink.example.com", 9000, allow_private_networks=True, resolver=resolver
    )

    assert addresses == ["10.0.0.5", "127.0.0.1", "93.184.216.34"]


async def test_resolve_public_addresses_still_requires_an_answer_when_flag_is_on() -> None:
    resolver = FakeResolver([])

    with pytest.raises(UnsafeTargetError):
        await resolve_public_addresses(
            "sink.example.com", 9000, allow_private_networks=True, resolver=resolver
        )


async def test_resolve_public_addresses_checks_ip_literals_without_dns_when_resolver_echoes_them() -> None:
    async def echo(host: str, port: int, /) -> list[str]:
        return [host]

    assert await resolve_public_addresses(
        "93.184.216.34", 443, allow_private_networks=False, resolver=echo
    ) == ["93.184.216.34"]
    with pytest.raises(UnsafeTargetError):
        await resolve_public_addresses("127.0.0.1", 443, allow_private_networks=False, resolver=echo)


async def test_resolve_public_addresses_default_resolver_rejects_loopback_name() -> None:
    # "localhost" is answered from the hosts file: no network access is needed.
    with pytest.raises(UnsafeTargetError):
        await resolve_public_addresses("localhost", 80, allow_private_networks=False)


async def test_resolve_public_addresses_default_resolver_handles_ip_literals() -> None:
    assert await resolve_public_addresses("93.184.216.34", 443, allow_private_networks=False) == [
        "93.184.216.34"
    ]
    with pytest.raises(UnsafeTargetError, match="non-public"):
        await resolve_public_addresses("127.0.0.1", 443, allow_private_networks=False)


@pytest.mark.parametrize("address", NEVER_ALLOWED_ADDRESSES)
@pytest.mark.parametrize("allow_private_networks", [False, True])
async def test_resolve_public_addresses_never_allows_dangerous_addresses_whatever_the_flag(
    address: str, allow_private_networks: bool
) -> None:
    resolver = FakeResolver([address])

    with pytest.raises(UnsafeTargetError):
        await resolve_public_addresses(
            "hooks.example.com", 443, allow_private_networks=allow_private_networks, resolver=resolver
        )


@pytest.mark.parametrize("address", NOT_PRIVATE_USE_NOR_PUBLIC_ADDRESSES)
async def test_resolve_public_addresses_with_the_flag_on_still_refuses_unlisted_special_ranges(
    address: str,
) -> None:
    with pytest.raises(UnsafeTargetError):
        await resolve_public_addresses(
            "hooks.example.com", 443, allow_private_networks=True, resolver=FakeResolver([address])
        )


async def test_resolve_public_addresses_refuses_a_name_resolving_to_the_metadata_address_with_flag_on() -> (
    None
):
    resolver = FakeResolver(["169.254.169.254"])

    with pytest.raises(UnsafeTargetError, match="never allowed"):
        await resolve_public_addresses(
            "innocent.example.com", 80, allow_private_networks=True, resolver=resolver
        )

    assert resolver.calls == [("innocent.example.com", 80)]


@pytest.mark.parametrize(
    "answer",
    [
        ["10.0.0.5", "169.254.169.254"],
        ["169.254.169.254", "10.0.0.5"],
        ["93.184.216.34", "127.0.0.1", "100.100.100.200"],
        ["10.0.0.5", "fd00:ec2::254"],
        ["192.168.1.10", "::ffff:169.254.169.254"],
        ["10.0.0.5", "fe80::1"],
    ],
)
async def test_resolve_public_addresses_is_all_or_nothing_for_mixed_answers_with_the_flag_on(
    answer: list[str],
) -> None:
    with pytest.raises(UnsafeTargetError):
        await resolve_public_addresses(
            "mixed.example.com", 443, allow_private_networks=True, resolver=FakeResolver(answer)
        )


async def test_resolve_public_addresses_with_the_flag_on_accepts_every_private_use_address() -> None:
    addresses = await resolve_public_addresses(
        "sink.example.com",
        9000,
        allow_private_networks=True,
        resolver=FakeResolver([*PRIVATE_USE_ADDRESSES, "93.184.216.34"]),
    )

    assert addresses == [*PRIVATE_USE_ADDRESSES, "93.184.216.34"]


@pytest.mark.parametrize("address", PRIVATE_USE_ADDRESSES)
async def test_resolve_public_addresses_without_the_flag_refuses_every_private_use_address(
    address: str,
) -> None:
    with pytest.raises(UnsafeTargetError, match="non-public"):
        await resolve_public_addresses(
            "sink.example.com", 9000, allow_private_networks=False, resolver=FakeResolver([address])
        )
