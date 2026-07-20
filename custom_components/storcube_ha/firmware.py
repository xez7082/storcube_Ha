"""Gestion des mises à jour de firmware pour StorCube."""
from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable

import aiohttp

from homeassistant.core import HomeAssistant

from .const import DEFAULT_APP_CODE, FIRMWARE_URL

_LOGGER = logging.getLogger(__name__)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 11; SM-A202F Build/RP1A.200720.012; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 "
    "Chrome/136.0.7103.60 Mobile Safari/537.36 uni-app Html5Plus/1.0 (Immersed/24.0)"
)

UNKNOWN = "Inconnue"


class StorCubeFirmwareManager:
    """Interroge l'API StorCube au sujet du firmware.

    Le gestionnaire ne s'authentifie pas lui-même : il réutilise la session
    et le token du coordinateur, qui reste le seul point d'authentification
    de l'intégration.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        device_id: str,
        session: aiohttp.ClientSession,
        token_provider: Callable[..., Awaitable[str]],
        app_code: str = DEFAULT_APP_CODE,
    ) -> None:
        """Initialiser le gestionnaire."""
        self.hass = hass
        self.device_id = device_id
        self.app_code = app_code
        self._session = session
        self._token_provider = token_provider
        self._cache: dict | None = None

    async def _async_headers(self) -> dict[str, str]:
        """Construire les en-têtes authentifiés."""
        token = await self._token_provider()
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "appCode": self.app_code,
            "accept-language": "fr-FR",
            "user-agent": USER_AGENT,
        }

    async def check_firmware_upgrade(self, retry: bool = True) -> dict | None:
        """Interroger l'API et retourner l'état du firmware."""
        url = FIRMWARE_URL + self.device_id

        try:
            async with self._session.get(
                url, headers=await self._async_headers(), timeout=REQUEST_TIMEOUT
            ) as response:
                if response.status in (401, 403) and retry:
                    _LOGGER.debug("Token rejeté par l'API firmware, renouvellement")
                    await self._token_provider(force_refresh=True)
                    return await self.check_firmware_upgrade(retry=False)
                response.raise_for_status()
                payload = await response.json(content_type=None)
        except aiohttp.ClientError as err:
            _LOGGER.debug("Vérification firmware impossible : %s", err)
            return None

        if payload.get("code") != 200:
            _LOGGER.debug(
                "Réponse firmware en erreur : %s", payload.get("message", "inconnue")
            )
            return None

        data = payload.get("data") or {}
        # Les libellés de l'API sont ambigus : ce log permet de confronter le
        # résultat à ce qu'affiche l'application mobile.
        _LOGGER.debug("Charge firmware brute : %s", data)

        installed = data.get("lastBigVersion") or UNKNOWN
        available = data.get("currentBigVersion") or installed
        upgrade_available = bool(data.get("upgread", False))

        notes: list[str] = []
        if upgrade_available:
            for remark in data.get("remarkList") or []:
                content = remark.get("remark", "")
                try:
                    notes.append(
                        json.loads(content).get(
                            "fr", "Notes non disponibles en français"
                        )
                    )
                except (json.JSONDecodeError, TypeError, AttributeError):
                    if content:
                        notes.append(content)

        self._cache = {
            "upgrade_available": upgrade_available,
            "current_version": installed,
            "latest_version": available,
            "firmware_notes": notes,
        }
        return self._cache

    async def get_firmware_info(self) -> dict:
        """Retourner le dernier état connu, sans nouvel appel réseau."""
        if self._cache is not None:
            return dict(self._cache)
        return {
            "current_version": UNKNOWN,
            "latest_version": UNKNOWN,
            "upgrade_available": False,
            "firmware_notes": [],
        }
