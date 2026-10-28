"""SSRF protection for outgoing webhooks.

A ``webhook_url`` is attacker-controlled input, and the service connects to it from inside the
deployment network. Two independent layers protect internal resources (cloud metadata endpoints,
databases, the broker, other services):

1. :func:`parse_webhook_url` / :func:`validate_webhook_url` - a purely syntactic check, run when the
   payment is accepted *and* again right before sending. It rejects odd schemes, credentials, ambiguous
   numeric hosts (``2130706433``, ``0x7f.1``, ``127.1``), internal host names and IP literals that the
   address policy below refuses.
2. :func:`resolve_public_addresses` - resolves the name and requires **every** returned address to pass
   the same address policy. The caller must then connect to one of the *returned* addresses instead of
   resolving the name again, otherwise a DNS rebinding attack could swap the answer between the check
   and the connection.

Address policy (one place, see ``_ALWAYS_FORBIDDEN_NETWORKS`` and ``_PRIVATE_USE_NETWORKS``). Layers are
evaluated in this order and the first match wins:

1. **Never allowed, whatever the flag says:** IPv4 link-local 169.254.0.0/16 (cloud metadata
   169.254.169.254, ECS 169.254.170.2) and IPv6 fe80::/10; "this network" 0.0.0.0/8 and ``::``; multicast;
   reserved / broadcast 240.0.0.0/4; CGNAT 100.64.0.0/10 (Alibaba metadata 100.100.100.200); the AWS IPv6
   metadata address fd00:ec2::254; Azure's 168.63.129.16; any IPv4-mapped or embedded (6to4, Teredo, NAT64)
   form of those; and the host names ``metadata``, ``metadata.google.internal`` and ``metadata.goog``.
2. **Globally routable unicast addresses are allowed.** IPv4-mapped IPv6 addresses never are.
3. **Only with ``allow_private_networks=True``** (docker / local development) private-use addresses are
   allowed as well: loopback 127.0.0.0/8 and ::1, RFC 1918 (10/8, 172.16/12, 192.168/16) and IPv6 ULA
   fc00::/7, plus non-global host names (``localhost``, ``*.localhost``, single-label compose service
   names, ``*.local``, ``*.internal`` ...). Everything else that is not globally routable (documentation
   and benchmarking ranges, ...) stays refused: the relaxed set is an allow-list, not "no checks".

With ``allow_private_networks=False`` only layers 1 and 2 apply, i.e. every non-global address and every
internal-looking host name is refused. Independently of the flag, the scheme, credentials, length, port,
host syntax and ambiguous-numeric-host checks always apply.

Everything here is pure (no I/O besides the injectable DNS lookup), so it is cheap to unit test.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol
from urllib.parse import quote, urlsplit

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

MAX_URL_LENGTH = 2048
"""Same bound as the ``webhook_url`` column (``VARCHAR(2048)``)."""

_MAX_HOSTNAME_LENGTH = 253
_MIN_PUBLIC_LABELS = 2
_MAX_PORT = 65535
_DEFAULT_PORTS = {"http": 80, "https": 443}

_FORBIDDEN_URL_CHARACTERS = re.compile(r"[\x00-\x20\x7f\\]")
"""Whitespace, control characters and backslashes: the classic sources of parser differentials."""

_NUMERIC_LABEL = re.compile(r"0[xX][0-9a-fA-F]*|[0-9]+")
"""A last host label like this makes WHATWG parsers and ``inet_aton`` treat the host as an IPv4 address."""

_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_LABEL_RELAXED = re.compile(r"[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?")
"""Docker/Compose service names may contain underscores; only accepted when private networks are allowed."""

_NEVER_ALLOWED_SUMMARY = "link-local, metadata, CGNAT, multicast, reserved or unspecified"

_ALWAYS_FORBIDDEN_NAMES = frozenset({"metadata", "metadata.google.internal", "metadata.goog"})
"""Cloud metadata service names; refused even when private networks are allowed."""

_FORBIDDEN_NAMES = ("localhost",)
_FORBIDDEN_SUFFIXES = (
    ".localhost",
    ".local",
    ".internal",
    ".localdomain",
    ".home.arpa",
    ".lan",
    ".intranet",
    ".corp",
)

_PATH_SAFE = "/%:@!$&'()*+,;=-._~"
_QUERY_SAFE = "/%:@!$&'()*+,;=?-._~"

_NAT64_PREFIX = ipaddress.IPv6Network("64:ff9b::/96")
_IPV4_COMPATIBLE_PREFIX = ipaddress.IPv6Network("::/96")

type _IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
type _IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


def _networks(*cidrs: str) -> tuple[_IPNetwork, ...]:
    return tuple(ipaddress.ip_network(cidr) for cidr in cidrs)


_ALWAYS_FORBIDDEN_NETWORKS = _networks(
    "0.0.0.0/8",  # "this network", includes the unspecified address
    "100.64.0.0/10",  # CGNAT shared space, includes Alibaba Cloud metadata 100.100.100.200
    "169.254.0.0/16",  # link-local: cloud metadata 169.254.169.254, ECS task metadata 169.254.170.2
    "168.63.129.16/32",  # Azure WireServer / host agent (a public-looking address)
    "224.0.0.0/4",  # multicast
    "240.0.0.0/4",  # reserved, includes the limited broadcast address 255.255.255.255
    "::/128",  # unspecified
    "fe80::/10",  # IPv6 link-local
    "ff00::/8",  # IPv6 multicast
    "fd00:ec2::254/128",  # AWS IPv6 instance metadata, inside the otherwise allowed ULA range
)
"""Refused whatever ``allow_private_networks`` says; also matched against IPv4 embedded in IPv6."""

_PRIVATE_USE_NETWORKS = _networks(
    "127.0.0.0/8",
    "::1/128",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "fc00::/7",
)
"""Accepted only with ``allow_private_networks=True`` (unless inside ``_ALWAYS_FORBIDDEN_NETWORKS``)."""


class UnsafeTargetError(ValueError):
    """The webhook target is malformed or points at something the service must never connect to.

    Messages are safe to show to API clients and to write to logs: they never contain credentials,
    the query string or the path of the rejected URL.
    """


class Resolver(Protocol):
    """Asynchronous name resolution: ``host, port`` -> IP address strings (injectable for tests)."""

    async def __call__(self, host: str, port: int, /) -> Sequence[str]:
        """Return the IP addresses of ``host`` (may raise ``OSError`` when it cannot be resolved)."""
        ...


@dataclass(frozen=True, slots=True)
class WebhookTarget:
    """A webhook URL that passed validation, split into the parts a sender needs.

    Every field is normalised (ASCII host in lower case, explicit port, percent-encoded path) so that the
    sender never has to re-parse the user-supplied string.
    """

    scheme: str
    host: str
    """ASCII host name or IP literal, lower case, without brackets and without a trailing dot."""
    port: int
    """Effective port (the scheme default when the URL does not name one)."""
    request_target: str
    """Path and query as sent on the request line; always starts with ``/``. The fragment is dropped."""

    @property
    def host_header(self) -> str:
        """Value of the ``Host`` header: bracketed IPv6, port only when it is not the scheme default."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        if self.port == _DEFAULT_PORTS[self.scheme]:
            return host
        return f"{host}:{self.port}"

    @property
    def url(self) -> str:
        """The normalised URL."""
        return f"{self.scheme}://{self.host_header}{self.request_target}"


def is_public_ip(ip: str) -> bool:
    """Whether ``ip`` is a globally routable unicast address that is not a known metadata endpoint.

    Equivalent to ``is_allowed_ip(ip, allow_private_networks=False)``. Anything that does not parse is
    *not* public. ``ipaddress.is_global`` alone is not enough: depending on the Python patch release it
    accepts multicast (``224.0.0.1``) and IPv4 addresses embedded in IPv6 (``::ffff:127.0.0.1``,
    ``64:ff9b::7f00:1``), so those are checked explicitly.
    """
    return is_allowed_ip(ip, allow_private_networks=False)


def is_allowed_ip(ip: str, *, allow_private_networks: bool) -> bool:
    """Whether the address policy (see the module docstring) lets the service connect to ``ip``.

    Anything that does not parse is refused.
    """
    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return _is_allowed(address, allow_private_networks=allow_private_networks)


def _is_allowed(address: _IPAddress, *, allow_private_networks: bool) -> bool:
    """The address policy in three layers; the first one that decides wins."""
    if _is_always_forbidden(address):
        return False
    if _is_public(address):
        return True
    return allow_private_networks and _is_private_use(address)


def _is_always_forbidden(address: _IPAddress) -> bool:
    candidates: list[_IPAddress] = [address]
    if isinstance(address, ipaddress.IPv6Address):
        candidates.extend(_embedded_ipv4(address))
    return any(candidate in network for candidate in candidates for network in _ALWAYS_FORBIDDEN_NETWORKS)


def _is_private_use(address: _IPAddress) -> bool:
    return any(address in network for network in _PRIVATE_USE_NETWORKS)


def _is_public(address: _IPAddress) -> bool:
    if (
        not address.is_global
        or address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        return False
    if isinstance(address, ipaddress.IPv6Address):
        # IPv4-mapped addresses are refused outright: they are never a legitimate AAAA record, and
        # dual-stack sockets would connect to the embedded IPv4 address.
        if address.ipv4_mapped is not None or address.is_site_local or address in _IPV4_COMPATIBLE_PREFIX:
            return False
        return all(_is_public(embedded) for embedded in _embedded_ipv4(address))
    return True


def _embedded_ipv4(address: ipaddress.IPv6Address) -> list[ipaddress.IPv4Address]:
    """IPv4 addresses a gateway would route to when it sees this IPv6 address."""
    embedded = [candidate for candidate in (address.ipv4_mapped, address.sixtofour) if candidate is not None]
    if address.teredo is not None:
        embedded.extend(address.teredo)
    if address in _NAT64_PREFIX:
        embedded.append(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF))
    return embedded


def parse_webhook_url(url: str, *, allow_private_networks: bool) -> WebhookTarget:
    """Validate ``url`` and return its normalised parts.

    Always enforced: ``http``/``https`` scheme, a host, no credentials, port 1-65535, length limit, no
    control characters or backslashes, a well-formed host, no ambiguous numeric (non-canonical IPv4) host,
    and the "never allowed" part of the address policy (link-local / metadata, CGNAT, multicast, reserved,
    unspecified addresses and cloud metadata host names). ``allow_private_networks=True`` relaxes only the
    private-use part: loopback, RFC 1918, ULA addresses and non-global host names (see the module docstring).

    Raises:
        UnsafeTargetError: if the URL is not acceptable.
    """
    if not url:
        msg = "URL is empty"
        raise UnsafeTargetError(msg)
    if len(url) > MAX_URL_LENGTH:
        msg = f"URL is longer than {MAX_URL_LENGTH} characters"
        raise UnsafeTargetError(msg)
    if _FORBIDDEN_URL_CHARACTERS.search(url):
        msg = "URL contains whitespace, control characters or backslashes"
        raise UnsafeTargetError(msg)

    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as exc:
        msg = "URL is malformed or its port is out of range"
        raise UnsafeTargetError(msg) from exc

    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS:
        msg = "URL scheme must be http or https"
        raise UnsafeTargetError(msg)
    if "@" in parts.netloc:
        msg = "URL must not contain credentials (user:password@)"
        raise UnsafeTargetError(msg)
    if not parts.hostname:
        msg = "URL has no host"
        raise UnsafeTargetError(msg)
    if parts.netloc.endswith(":"):
        msg = "URL has an empty port"
        raise UnsafeTargetError(msg)
    if port is None:
        port = _DEFAULT_PORTS[scheme]
    elif not 1 <= port <= _MAX_PORT:
        msg = "URL port must be between 1 and 65535"
        raise UnsafeTargetError(msg)

    host = _normalise_host(parts.hostname, allow_private_networks=allow_private_networks)
    target = WebhookTarget(
        scheme=scheme,
        host=host,
        port=port,
        request_target=_request_target(parts.path, parts.query),
    )
    if len(target.url) > MAX_URL_LENGTH:
        msg = f"normalised URL is longer than {MAX_URL_LENGTH} characters"
        raise UnsafeTargetError(msg)
    return target


def validate_webhook_url(url: str, *, allow_private_networks: bool) -> str:
    """Validate ``url`` (see :func:`parse_webhook_url`) and return it in normalised form.

    Raises:
        UnsafeTargetError: if the URL is not acceptable.
    """
    return parse_webhook_url(url, allow_private_networks=allow_private_networks).url


def _request_target(path: str, query: str) -> str:
    target = quote(path, safe=_PATH_SAFE) or "/"
    if query:
        target = f"{target}?{quote(query, safe=_QUERY_SAFE)}"
    return target


def _normalise_host(raw_host: str, *, allow_private_networks: bool) -> str:
    if "%" in raw_host:
        msg = "host must not contain percent-encoding or an IPv6 zone identifier"
        raise UnsafeTargetError(msg)

    if ":" in raw_host:
        try:
            ipv6 = ipaddress.IPv6Address(raw_host)
        except ValueError as exc:
            msg = "host is not a valid IPv6 address"
            raise UnsafeTargetError(msg) from exc
        _require_allowed(ipv6, allow_private_networks=allow_private_networks)
        return ipv6.compressed

    host = _to_ascii(raw_host).lower().removesuffix(".")
    if not host or len(host) > _MAX_HOSTNAME_LENGTH:
        msg = "host name is empty or too long"
        raise UnsafeTargetError(msg)

    ipv4 = _parse_canonical_ipv4(host)
    if ipv4 is not None:
        _require_allowed(ipv4, allow_private_networks=allow_private_networks)
        return str(ipv4)

    labels = host.split(".")
    if _NUMERIC_LABEL.fullmatch(labels[-1]):
        # Looks like a number, is not a canonical dotted quad: "2130706433", "0x7f.1", "127.1", "017.0.0.1".
        # Different parsers (inet_aton, browsers, libraries) disagree on what these mean, so refuse them.
        msg = "ambiguous numeric host: use a canonical dotted-decimal IPv4 address or a host name"
        raise UnsafeTargetError(msg)

    label_pattern = _LABEL_RELAXED if allow_private_networks else _LABEL
    if not all(label_pattern.fullmatch(label) for label in labels):
        msg = "host name contains invalid characters or labels"
        raise UnsafeTargetError(msg)

    _require_allowed_name(host, labels, allow_private_networks=allow_private_networks)
    return host


def _parse_canonical_ipv4(host: str) -> ipaddress.IPv4Address | None:
    """Strict dotted-decimal parsing: exactly four octets, no leading zeros, no shorthand."""
    try:
        return ipaddress.IPv4Address(host)
    except ValueError:
        return None


def _to_ascii(host: str) -> str:
    """IDNA-encode non-ASCII hosts so the checks (and the connection) see what DNS will see."""
    if host.isascii():
        return host
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        msg = "host name is not valid IDNA"
        raise UnsafeTargetError(msg) from exc


def _require_allowed(address: _IPAddress, *, allow_private_networks: bool) -> None:
    if not _is_allowed(address, allow_private_networks=allow_private_networks):
        if allow_private_networks:
            msg = f"host is an IP address in a range that is never allowed ({_NEVER_ALLOWED_SUMMARY})"
        else:
            msg = "host is an IP address that is not publicly routable"
        raise UnsafeTargetError(msg)


def _require_allowed_name(host: str, labels: Sequence[str], *, allow_private_networks: bool) -> None:
    if host in _ALWAYS_FORBIDDEN_NAMES:
        msg = f"host name {host!r} is a cloud metadata service"
        raise UnsafeTargetError(msg)
    if allow_private_networks:
        return
    if host in _FORBIDDEN_NAMES or host.endswith(_FORBIDDEN_SUFFIXES):
        msg = f"host name {host!r} refers to a local or internal network"
        raise UnsafeTargetError(msg)
    if len(labels) < _MIN_PUBLIC_LABELS:
        msg = f"host name {host!r} is not a fully qualified domain name"
        raise UnsafeTargetError(msg)


async def system_resolver(host: str, port: int, /) -> list[str]:
    """Resolve ``host`` with the operating system resolver without blocking the event loop."""
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


async def resolve_public_addresses(
    host: str,
    port: int,
    *,
    allow_private_networks: bool,
    resolver: Resolver | None = None,
) -> list[str]:
    """Resolve ``host`` and return its addresses, refusing the whole answer if any of them is unsafe.

    All-or-nothing on purpose: an attacker-controlled DNS zone can mix a public record with a private one
    and hope the client picks the private one. The result is de-duplicated and keeps the resolver's order.
    Connect to one of the returned addresses (not to the name) to stay immune to DNS rebinding.

    Args:
        host: Host name or IP literal (as returned by :func:`parse_webhook_url`).
        port: Destination port, passed to the resolver.
        allow_private_networks: Also accept loopback / RFC 1918 / ULA addresses (local demos and tests
            only); the "never allowed" ranges of the module docstring stay refused.
        resolver: Replacement for the operating system resolver.

    Raises:
        UnsafeTargetError: if resolution fails, yields nothing or yields an invalid / non-public address.
    """
    resolve = system_resolver if resolver is None else resolver
    try:
        answers = await resolve(host, port)
    except OSError as exc:
        msg = f"cannot resolve host {host!r} ({type(exc).__name__})"
        raise UnsafeTargetError(msg) from exc

    addresses = _unique(answers)
    if not addresses:
        msg = f"host {host!r} did not resolve to any address"
        raise UnsafeTargetError(msg)

    for address in addresses:
        try:
            parsed = ipaddress.ip_address(address)
        except ValueError as exc:
            msg = f"host {host!r} resolved to an invalid address"
            raise UnsafeTargetError(msg) from exc
        if not _is_allowed(parsed, allow_private_networks=allow_private_networks):
            if allow_private_networks:
                msg = f"host {host!r} resolves to an address that is never allowed ({address})"
            else:
                msg = f"host {host!r} resolves to a non-public address ({address})"
            raise UnsafeTargetError(msg)
    return addresses


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))
