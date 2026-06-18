# SPDX-License-Identifier: 0BSD

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Optional

import RNS

from meshchatx.src.backend import reticulum_pathfinding


# Cache of established RNS Links keyed by (aspect_str, destination_hash_bytes).
# Kept separate from nomadnet_downloader.nomadnet_cached_links — the two caches
# may merge in the future if NomadNet is ported onto this generic Links API.
rns_cached_links: dict[tuple[str, bytes], "RNS.Link"] = {}
_rns_links_lock = threading.Lock()

# Wait granularity while polling for path / link (seconds).
_POLL_INTERVAL_S = 0.02


def get_cached_active_link(aspect: str, destination_hash: bytes):
    """Return a cached link if present and ACTIVE; drop stale entries."""
    key = (aspect, destination_hash)
    with _rns_links_lock:
        link = rns_cached_links.get(key)
        if link is None:
            return None
        if link.status is RNS.Link.ACTIVE:
            return link
        try:
            del rns_cached_links[key]
        except KeyError:
            pass
        return None


def sweep_stale_links():
    with _rns_links_lock:
        stale = [k for k, v in rns_cached_links.items() if v.status is not RNS.Link.ACTIVE]
        for k in stale:
            del rns_cached_links[k]


def _cache_link_if_active(aspect: str, destination_hash: bytes, link) -> None:
    if link is None or link.status is not RNS.Link.ACTIVE:
        return
    with _rns_links_lock:
        rns_cached_links[(aspect, destination_hash)] = link


def _uncache_link_if_matches(aspect: str, destination_hash: bytes, link) -> None:
    if link is None:
        return
    key = (aspect, destination_hash)
    with _rns_links_lock:
        if rns_cached_links.get(key) is link:
            try:
                del rns_cached_links[key]
            except KeyError:
                pass


def _split_aspect(aspect: str) -> tuple[str, list[str]]:
    parts = [p for p in aspect.split(".") if p]
    if not parts:
        raise ValueError("aspect must be a non-empty dot-separated string")
    return parts[0], parts[1:]


class RnsLinkManager:
    """Generic RNS Link lifecycle / request / packet helper.

    The web layer wires three callables:
      - self_identity_getter: returns the local RNS.Identity (or None).
      - reticulum_getter: returns the MeshChat reticulum-like (used by
        reticulum_pathfinding.prepare_fresh_path_request).
      - broadcast_event: called with a JSON-serializable dict; expected to
        forward to all interested /ws clients.
    """

    def __init__(
        self,
        *,
        self_identity_getter: Callable[[], Optional["RNS.Identity"]],
        reticulum_getter: Callable[[], object],
        broadcast_event: Callable[[dict], None],
    ):
        self._get_identity = self_identity_getter
        self._get_reticulum = reticulum_getter
        self._broadcast = broadcast_event

    async def open_link(
        self,
        destination_hash: bytes,
        aspect: str,
        *,
        auto_identify: bool = False,
        on_phase: Optional[Callable[[str], None]] = None,
        path_lookup_timeout: float = 15.0,
        link_establishment_timeout: float = 15.0,
    ) -> tuple[Optional["RNS.Link"], bool, Optional[str]]:
        """Open (or reuse) a Link to (aspect, destination_hash).

        Returns (link, identified, failure_reason). On failure link is None
        and failure_reason is set; otherwise failure_reason is None.
        """
        app_name, sub_aspects = _split_aspect(aspect)

        def _phase(p: str) -> None:
            if on_phase is not None:
                try:
                    on_phase(p)
                except Exception:
                    pass

        cached = get_cached_active_link(aspect, destination_hash)
        if cached is not None:
            identified = False
            if auto_identify:
                identity = self._get_identity()
                if identity is None:
                    return None, False, "no_local_identity"
                _phase("identifying")
                try:
                    cached.identify(identity)
                    identified = True
                except Exception as e:
                    return None, False, f"identify_failed: {e}"
            return cached, identified, None

        # Path lookup
        reticulum_pathfinding.prepare_fresh_path_request(
            self._get_reticulum(),
            destination_hash,
        )
        if not RNS.Transport.has_path(destination_hash):
            _phase("finding_path")
            deadline = time.time() + path_lookup_timeout
            while (
                not RNS.Transport.has_path(destination_hash)
                and time.time() < deadline
            ):
                await asyncio.sleep(_POLL_INTERVAL_S)
        if not RNS.Transport.has_path(destination_hash):
            return None, False, "no_path_to_destination"

        # Re-check cache after path discovery (some other request may have
        # established a link in parallel).
        cached = get_cached_active_link(aspect, destination_hash)
        if cached is not None:
            identified = False
            if auto_identify:
                identity = self._get_identity()
                if identity is None:
                    return None, False, "no_local_identity"
                _phase("identifying")
                try:
                    cached.identify(identity)
                    identified = True
                except Exception as e:
                    return None, False, f"identify_failed: {e}"
            return cached, identified, None

        _phase("establishing_link")
        identity = RNS.Identity.recall(destination_hash)
        if identity is None:
            return None, False, "no_identity_for_destination"
        destination = RNS.Destination(
            identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            app_name,
            *sub_aspects,
        )

        link = RNS.Link(destination)
        link.set_packet_callback(
            lambda data, packet, _aspect=aspect, _dh=destination_hash: self._on_packet(
                _aspect, _dh, data,
            ),
        )
        link.set_link_closed_callback(
            lambda lnk, _aspect=aspect, _dh=destination_hash: self._on_link_closed(
                _aspect, _dh, lnk,
            ),
        )

        deadline = time.time() + link_establishment_timeout
        while link.status is not RNS.Link.ACTIVE and time.time() < deadline:
            await asyncio.sleep(_POLL_INTERVAL_S)
        if link.status is not RNS.Link.ACTIVE:
            try:
                link.teardown()
            except Exception:
                pass
            return None, False, "link_establishment_timeout"

        _cache_link_if_active(aspect, destination_hash, link)

        identified = False
        if auto_identify:
            identity_local = self._get_identity()
            if identity_local is None:
                return None, False, "no_local_identity"
            _phase("identifying")
            try:
                link.identify(identity_local)
                identified = True
            except Exception as e:
                return None, False, f"identify_failed: {e}"

        return link, identified, None

    def identify(self, destination_hash: bytes, aspect: str) -> tuple[bool, Optional[str]]:
        link = get_cached_active_link(aspect, destination_hash)
        if link is None:
            return False, "no_active_link"
        identity = self._get_identity()
        if identity is None:
            return False, "no_local_identity"
        try:
            link.identify(identity)
        except Exception as e:
            return False, f"identify_failed: {e}"
        return True, None

    def request(
        self,
        destination_hash: bytes,
        aspect: str,
        path: str,
        data,
        response_callback,
        failed_callback,
        progress_callback,
        timeout: Optional[float] = None,
    ):
        link = get_cached_active_link(aspect, destination_hash)
        if link is None:
            raise RuntimeError("no_active_link")
        return link.request(
            path,
            data=data,
            response_callback=response_callback,
            failed_callback=failed_callback,
            progress_callback=progress_callback,
            timeout=timeout,
        )

    def send_packet(self, destination_hash: bytes, aspect: str, payload: bytes) -> tuple[bool, Optional[str]]:
        link = get_cached_active_link(aspect, destination_hash)
        if link is None:
            return False, "no_active_link"
        try:
            RNS.Packet(link, payload).send()
        except Exception as e:
            return False, f"send_failed: {e}"
        return True, None

    def close(self, destination_hash: bytes, aspect: str) -> bool:
        link = get_cached_active_link(aspect, destination_hash)
        if link is None:
            return False
        _uncache_link_if_matches(aspect, destination_hash, link)
        try:
            link.teardown()
        except Exception:
            pass
        return True

    def _on_packet(self, aspect: str, destination_hash: bytes, data: bytes) -> None:
        import base64
        try:
            self._broadcast({
                "type": "rns.link.event",
                "event": "packet_received",
                "destination_hash": destination_hash.hex(),
                "aspect": aspect,
                "payload_b64": base64.b64encode(bytes(data)).decode("ascii"),
            })
        except Exception:
            pass

    def _on_link_closed(self, aspect: str, destination_hash: bytes, link) -> None:
        _uncache_link_if_matches(aspect, destination_hash, link)
        try:
            self._broadcast({
                "type": "rns.link.event",
                "event": "link_closed",
                "destination_hash": destination_hash.hex(),
                "aspect": aspect,
            })
        except Exception:
            pass
