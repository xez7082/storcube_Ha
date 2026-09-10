"""Coordinateur de données pour l'intégration Storcube Battery Monitor."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta

import aiohttp
import websockets

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import (
    CONF_APP_CODE,
    CONF_AUTH_PASSWORD,
    CONF_DEVICE_ID,
    CONF_LOGIN_NAME,
    DEFAULT_APP_CODE,
    DOMAIN,
    MAX_POWER,
    MIN_POWER,
    OUTPUT_URL,
    QUERY_THRESHOLD_URL,
    SET_POWER_URL,
    SET_THRESHOLD_URL,
    TOKEN_URL,
    TOPIC_BATTERY_CAPACITY,
    TOPIC_BATTERY_POWER,
    TOPIC_BATTERY_REPORT,
    TOPIC_BATTERY_SOLAR,
    TOPIC_BATTERY_STATUS,
    TOPIC_FIRMWARE,
    TOPIC_OUTPUT,
    TOPIC_OUTPUT_POWER,
    TOPIC_THRESHOLD,
    WS_URI,
)
from .firmware import StorCubeFirmwareManager

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Temporisations
# ---------------------------------------------------------------------------

# Intervalle nominal de la boucle REST.
REST_INTERVAL = 30

# Bornes du backoff REST en cas d'échec.
REST_BACKOFF_MAX = 600

# Vérification firmware tous les N cycles REST réussis.
# 20 x 30 s = 10 minutes.
FIRMWARE_EVERY = 20

# Durée de validité supposée du token.
# L'API ne renvoie pas d'expiration.
TOKEN_TTL = timedelta(hours=12)

# Délai de reconnexion du WebSocket.
WS_RETRY_MIN = 5
WS_RETRY_MAX = 300

# Sans trame reçue pendant ce délai, on relance l'abonnement.
WS_HEARTBEAT = 30

# Au-delà, les données temps réel sont considérées comme périmées.
WS_FRESH_MAX = 180

# Version du stockage persistant.
STORAGE_VERSION = 1


class StorCubeDataUpdateCoordinator(DataUpdateCoordinator):
    """Coordinateur des données REST et WebSocket StorCube."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialiser le coordinateur."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            # Les données sont poussées par REST/WebSocket.
            update_interval=timedelta(seconds=15),
        )

        # ------------------------------------------------------------------
        # Données internes
        # ------------------------------------------------------------------

        self._raw: dict[str, dict] = {
            "websocket": {},
            "rest_api": {},
            "firmware": {},
        }

        self._last_ws_update: str | None = None
        self._last_rest_update: str | None = None

        self._session = async_get_clientsession(hass)

        self._store = Store(
            hass,
            STORAGE_VERSION,
            f"{DOMAIN}_token",
        )

        self._auth_token: str | None = None
        self._token_expires_at: datetime | None = None
        self._token_lock = asyncio.Lock()

        # Tous les equipId connus.
        #
        # Le maître est fourni par la configuration.
        # Les esclaves sont découverts via scene/user/list/V2.
        # Des equipId supplémentaires peuvent aussi être découverts
        # directement dans les trames WebSocket.
        self._known_devices: set[str] = set()

        # Champ utilisé pour le seuil de décharge.
        self._threshold_field: str | None = None

        # MQTT.
        self._mqtt_available = False

        # Tâches principales.
        self._ws_task: asyncio.Task | None = None
        self._rest_task: asyncio.Task | None = None

        # Connexion WebSocket actuellement active.
        self._ws_connection = None

        # Protection des websocket.send().
        self._ws_send_lock = asyncio.Lock()

        # Événement déclenché lorsqu'un nouvel equipId est découvert.
        #
        # La boucle WebSocket réémet alors immédiatement l'abonnement
        # avec la nouvelle liste d'appareils.
        self._ws_devices_changed = asyncio.Event()

        # Gestionnaire firmware.
        self.firmware_manager = StorCubeFirmwareManager(
            api_call=self._async_api_call,
            device_id=config_entry.data[CONF_DEVICE_ID],
            app_code=config_entry.data.get(
                CONF_APP_CODE,
                DEFAULT_APP_CODE,
            ),
        )

        # Le maître est connu immédiatement.
        master_id = str(
            config_entry.data[CONF_DEVICE_ID]
        ).strip()

        if master_id:
            self._known_devices.add(master_id)

        _LOGGER.debug(
            "Coordinateur StorCube initialisé "
            "(device_id=%s, login=%s)",
            config_entry.data[CONF_DEVICE_ID],
            config_entry.data[CONF_LOGIN_NAME],
        )

    # ------------------------------------------------------------------
    # Propriétés exposées aux entités
    # ------------------------------------------------------------------

    @property
    def master_equip_id(self) -> str:
        """Identifiant de la batterie maître."""
        return str(
            self.config_entry.data[CONF_DEVICE_ID]
        )

    @property
    def known_devices(self) -> set[str]:
        """Retourner les equipId actuellement connus."""
        return set(self._known_devices)

    @property
    def firmware(self) -> dict:
        """Retourner les informations firmware courantes."""
        return self._raw["firmware"]

    @property
    def last_rest_update(self) -> str | None:
        """Horodatage de la dernière mise à jour REST."""
        return self._last_rest_update

    @property
    def last_ws_update(self) -> str | None:
        """Horodatage de la dernière mise à jour WebSocket."""
        return self._last_ws_update

    # ------------------------------------------------------------------
    # Gestion des appareils / stack
    # ------------------------------------------------------------------

    def _register_known_equip_id(
        self,
        equip_id: str | int | None,
        source: str,
    ) -> bool:
        """Ajouter un equipId à la liste des appareils connus."""
        if equip_id is None:
            return False

        equip_id = str(equip_id).strip()

        if not equip_id:
            return False

        if equip_id in self._known_devices:
            return False

        self._known_devices.add(equip_id)

        _LOGGER.info(
            "Nouvelle batterie StorCube détectée via %s : %s",
            source,
            equip_id,
        )

        # Demande à la boucle WebSocket de réémettre l'abonnement.
        self._ws_devices_changed.set()

        return True

    def _discover_stack_from_scene(
        self,
        scene_data: dict,
    ) -> set[str]:
        """Découvrir toute la pile depuis les données REST.

        Exemple de réponse observée :

            {
                "equipId": "9106240712491153",
                "equipIds": [
                    "9106240712491153",
                    "9106240712491331"
                ]
            }

        Le REST sert uniquement à découvrir la pile et les informations
        opérationnelles. Le SoC, la puissance, la température et les autres
        données temps réel continuent à provenir du WebSocket.
        """
        discovered: set[str] = set()

        if not isinstance(scene_data, dict):
            return discovered

        # --------------------------------------------------------------
        # Maître
        # --------------------------------------------------------------

        master_id = scene_data.get("equipId")

        if master_id is not None:
            master_id = str(master_id).strip()

            if master_id:
                discovered.add(master_id)

                self._register_known_equip_id(
                    master_id,
                    "REST scene",
                )

        # --------------------------------------------------------------
        # Ensemble de la pile
        # --------------------------------------------------------------

        equip_ids = scene_data.get("equipIds")

        if isinstance(
            equip_ids,
            (list, tuple, set),
        ):
            for equip_id in equip_ids:
                if equip_id is None:
                    continue

                equip_id = str(equip_id).strip()

                if not equip_id:
                    continue

                discovered.add(equip_id)

                self._register_known_equip_id(
                    equip_id,
                    "REST equipIds",
                )

        return discovered

    def _build_ws_subscribe_payload(self) -> str:
        """Construire l'abonnement WebSocket pour toute la pile."""
        equip_ids = sorted(
            self._known_devices
        )

        payload = {
            "cmd": "sub",
            "action": "report",
            "reportEquip": equip_ids,
        }

        return json.dumps(payload)

    async def _async_send_ws_subscribe(
        self,
        websocket,
    ) -> bool:
        """Envoyer l'abonnement WebSocket courant."""
        if websocket is None:
            return False

        if not self._known_devices:
            _LOGGER.debug(
                "Abonnement WebSocket impossible : "
                "aucun equipId connu"
            )
            return False

        subscribe = self._build_ws_subscribe_payload()

        try:
            async with self._ws_send_lock:
                await websocket.send(subscribe)

            _LOGGER.debug(
                "Abonnement WebSocket envoyé pour %s appareil(s) : %s",
                len(self._known_devices),
                subscribe,
            )

            return True

        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "Envoi abonnement WebSocket impossible : %s",
                err,
            )
            return False

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    async def async_setup(self) -> bool:
        """Démarrer le coordinateur."""
        _LOGGER.debug(
            "Configuration du coordinateur StorCube"
        )

        # Authentification initiale.
        try:
            await self._async_get_token()

        except ConfigEntryAuthFailed:
            raise

        except Exception as err:
            raise ConfigEntryNotReady(
                f"Impossible de joindre l'API StorCube : {err}"
            ) from err

        # MQTT est optionnel.
        self._mqtt_available = (
            await self._async_check_mqtt()
        )

        # Vérification firmware initiale.
        try:
            await self.async_check_firmware_upgrade()

        except Exception as err:  # noqa: BLE001
            _LOGGER.warning(
                "Vérification firmware initiale échouée : %s",
                err,
            )

        # Tâche WebSocket.
        self._ws_task = (
            self.config_entry.async_create_background_task(
                self.hass,
                self._websocket_loop(),
                name=f"{DOMAIN}_websocket",
            )
        )

        # Tâche REST.
        self._rest_task = (
            self.config_entry.async_create_background_task(
                self.hass,
                self._rest_loop(),
                name=f"{DOMAIN}_rest",
            )
        )

        _LOGGER.debug(
            "Coordinateur StorCube configuré"
        )

        return True

    async def async_shutdown(self) -> None:
        """Arrêter proprement le coordinateur."""
        _LOGGER.debug(
            "Arrêt du coordinateur StorCube"
        )

        for task in (
            self._ws_task,
            self._rest_task,
        ):
            if task and not task.done():
                task.cancel()

                try:
                    await task

                except asyncio.CancelledError:
                    pass

        self._ws_task = None
        self._rest_task = None
        self._ws_connection = None

        await super().async_shutdown()

    # ------------------------------------------------------------------
    # Agrégation des données
    # ------------------------------------------------------------------

    def _ws_is_fresh(self) -> bool:
        """Indiquer si les données WebSocket sont récentes."""
        if not self._last_ws_update:
            return False

        try:
            age = (
                datetime.now()
                - datetime.fromisoformat(
                    self._last_ws_update
                )
            )

        except (TypeError, ValueError):
            return False

        return age.total_seconds() < WS_FRESH_MAX

    async def _async_update_data(
        self,
    ) -> dict[str, dict]:
        """Combiner REST et WebSocket par equipId."""
        try:
            firmware_version = (
                self._raw["firmware"].get(
                    "current_version"
                )
            )

            fresh = self._ws_is_fresh()

            combined: dict[str, dict] = {}

            for equip_id in self._known_devices:
                merged = dict(
                    self._raw["rest_api"].get(
                        equip_id,
                        {},
                    )
                )

                # Le WebSocket est prioritaire.
                merged.update(
                    self._raw["websocket"].get(
                        equip_id,
                        {},
                    )
                )

                if (
                    firmware_version
                    and firmware_version != "Inconnue"
                ):
                    merged.setdefault(
                        "firmware_version",
                        firmware_version,
                    )

                merged["ws_fresh"] = (
                    fresh
                    and equip_id
                    in self._raw["websocket"]
                )

                combined[equip_id] = merged

            return combined

        except Exception as err:  # noqa: BLE001
            raise UpdateFailed(
                f"Erreur d'agrégation des données : {err}"
            ) from err

    # ------------------------------------------------------------------
    # Authentification
    # ------------------------------------------------------------------

    async def _async_get_token(
        self,
        force_refresh: bool = False,
    ) -> str:
        """Retourner un token valide."""
        async with self._token_lock:

            if (
                not force_refresh
                and self._auth_token
                and not self._token_expired()
            ):
                return self._auth_token

            # ----------------------------------------------------------
            # Restauration du token sauvegardé.
            # ----------------------------------------------------------

            if (
                not force_refresh
                and self._auth_token is None
            ):
                stored = await self._store.async_load()

                if stored and stored.get("token"):
                    expires_raw = stored.get(
                        "expires_at"
                    )

                    try:
                        expires = (
                            datetime.fromisoformat(
                                expires_raw
                            )
                            if expires_raw
                            else None
                        )

                    except ValueError:
                        expires = None

                    if (
                        expires
                        and expires > datetime.now()
                    ):
                        self._auth_token = (
                            stored["token"]
                        )

                        self._token_expires_at = (
                            expires
                        )

                        return self._auth_token

            # ----------------------------------------------------------
            # Nouveau login.
            # ----------------------------------------------------------

            credentials = {
                "appCode": self.config_entry.data.get(
                    CONF_APP_CODE,
                    DEFAULT_APP_CODE,
                ),
                "loginName": self.config_entry.data[
                    CONF_LOGIN_NAME
                ],
                "password": self.config_entry.data[
                    CONF_AUTH_PASSWORD
                ],
            }

            headers = {
                "Content-Type": "application/json"
            }

            try:
                async with self._session.post(
                    TOKEN_URL,
                    json=credentials,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(
                        total=20
                    ),
                ) as resp:

                    resp.raise_for_status()

                    payload = await resp.json(
                        content_type=None
                    )

            except aiohttp.ClientError as err:
                raise ConfigEntryNotReady(
                    f"API StorCube injoignable : {err}"
                ) from err

            if payload.get("code") != 200:
                raise ConfigEntryAuthFailed(
                    "Échec d'authentification : "
                    f"{payload.get('message', 'réponse inconnue')}"
                )

            token = (
                payload.get("data") or {}
            ).get("token")

            if not token:
                raise ConfigEntryAuthFailed(
                    "Token absent de la réponse de l'API"
                )

            self._auth_token = token

            self._token_expires_at = (
                datetime.now() + TOKEN_TTL
            )

            await self._store.async_save(
                {
                    "token": token,
                    "expires_at": (
                        self._token_expires_at.isoformat()
                    ),
                }
            )

            _LOGGER.debug(
                "Token StorCube renouvelé"
            )

            return token

    def _token_expired(self) -> bool:
        """Indiquer si le token est expiré."""
        if self._token_expires_at is None:
            return True

        return (
            datetime.now()
            >= self._token_expires_at
        )

    async def _async_headers(
        self,
    ) -> dict[str, str]:
        """Construire les headers REST."""
        token = await self._async_get_token()

        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "appCode": self.config_entry.data.get(
                CONF_APP_CODE,
                DEFAULT_APP_CODE,
            ),
        }

    # ------------------------------------------------------------------
    # API REST
    # ------------------------------------------------------------------

    async def _async_api_call(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        retry: bool = True,
    ) -> dict | None:
        """Appeler l'API REST."""
        headers = await self._async_headers()

        try:
            async with self._session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=aiohttp.ClientTimeout(
                    total=20
                ),
            ) as resp:

                if (
                    resp.status in (401, 403)
                    and retry
                ):
                    _LOGGER.debug(
                        "Token rejeté (%s), renouvellement",
                        resp.status,
                    )

                    await self._async_get_token(
                        force_refresh=True
                    )

                    return await self._async_api_call(
                        method,
                        url,
                        params=params,
                        json_body=json_body,
                        retry=False,
                    )

                resp.raise_for_status()

                return await resp.json(
                    content_type=None
                )

        except aiohttp.ClientError as err:
            _LOGGER.debug(
                "Erreur API %s %s : %s",
                method,
                url,
                err,
            )

            raise

    async def async_get_scene_data(
        self,
    ) -> dict | None:
        """Récupérer les données de scène."""
        url = (
            OUTPUT_URL
            + self.config_entry.data[
                CONF_DEVICE_ID
            ]
        )

        payload = await self._async_api_call(
            "GET",
            url,
        )

        if (
            not payload
            or payload.get("code") != 200
        ):
            return None

        scene_list = (
            payload.get("data") or []
        )

        return (
            scene_list[0]
            if scene_list
            else None
        )

    async def async_set_power_value(
        self,
        new_power_value,
    ) -> bool:
        """Modifier la consigne de puissance."""
        try:
            value = int(new_power_value)

        except (TypeError, ValueError):
            _LOGGER.error(
                "Consigne de puissance invalide : %r",
                new_power_value,
            )
            return False

        if not MIN_POWER <= value <= MAX_POWER:
            _LOGGER.error(
                "Consigne de puissance hors bornes "
                "(%s-%s W) : %s",
                MIN_POWER,
                MAX_POWER,
                value,
            )
            return False

        try:
            payload = await self._async_api_call(
                "GET",
                SET_POWER_URL,
                params={
                    "equipId": (
                        self.config_entry.data[
                            CONF_DEVICE_ID
                        ]
                    ),
                    "power": value,
                },
            )

        except aiohttp.ClientError as err:
            _LOGGER.error(
                "Erreur lors de la modification "
                "de la puissance : %s",
                err,
            )
            return False

        if (
            payload
            and payload.get("code") == 200
        ):
            _LOGGER.debug(
                "Puissance mise à jour : %s W",
                value,
            )

            await self.async_request_refresh()

            return True

        _LOGGER.error(
            "Échec de la mise à jour de la puissance : %s",
            (payload or {}).get("message"),
        )

        return False

    async def async_set_threshold_value(
        self,
        new_threshold_value,
    ) -> bool:
        """Modifier le seuil de décharge."""
        try:
            value = int(
                new_threshold_value
            )

        except (TypeError, ValueError):
            _LOGGER.error(
                "Seuil invalide : %r",
                new_threshold_value,
            )
            return False

        if not 0 <= value <= 100:
            _LOGGER.error(
                "Seuil hors bornes (0-100 %%) : %s",
                value,
            )
            return False

        equip_id = (
            self.config_entry.data[
                CONF_DEVICE_ID
            ]
        )

        fields = (
            [self._threshold_field]
            if self._threshold_field
            else [
                "reserved",
                "threshold",
                "data",
            ]
        )

        for field in fields:
            body = {
                field: str(value),
                "equipId": equip_id,
            }

            try:
                payload = (
                    await self._async_api_call(
                        "POST",
                        SET_THRESHOLD_URL,
                        json_body=body,
                    )
                )

            except aiohttp.ClientError as err:
                _LOGGER.error(
                    "Erreur lors de la modification "
                    "du seuil : %s",
                    err,
                )
                return False

            if (
                payload
                and payload.get("code") == 200
            ):
                if (
                    self._threshold_field
                    != field
                ):
                    _LOGGER.info(
                        "Champ de seuil retenu "
                        "pour l'API : %s",
                        field,
                    )

                self._threshold_field = field

                await self.async_request_refresh()

                return True

            _LOGGER.debug(
                "Champ de seuil %s refusé : %s",
                field,
                (payload or {}).get(
                    "message"
                ),
            )

        self._threshold_field = None

        _LOGGER.error(
            "Aucune variante de champ acceptée "
            "pour le seuil"
        )

        return False

    async def async_get_threshold(
        self,
    ) -> int | None:
        """Lire le seuil de décharge."""
        try:
            payload = await self._async_api_call(
                "GET",
                QUERY_THRESHOLD_URL,
                params={
                    "equipId": (
                        self.config_entry.data[
                            CONF_DEVICE_ID
                        ]
                    )
                },
            )

        except aiohttp.ClientError as err:
            _LOGGER.debug(
                "Lecture du seuil impossible : %s",
                err,
            )
            return None

        if not payload:
            return None

        try:
            return int(
                payload["data"]
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ):
            _LOGGER.debug(
                "Réponse de seuil inattendue : %s",
                payload,
            )

            return None

    # Alias rétrocompatibles.
    set_power_value = async_set_power_value
    set_threshold_value = async_set_threshold_value
    get_scene_data = async_get_scene_data

    # ------------------------------------------------------------------
    # Firmware
    # ------------------------------------------------------------------

    async def async_check_firmware_upgrade(
        self,
    ) -> dict | None:
        """Vérifier la disponibilité d'une mise à jour."""
        firmware_info = (
            await self.firmware_manager.check_firmware_upgrade()
        )

        if not firmware_info:
            _LOGGER.debug(
                "Aucune information firmware disponible"
            )

            return None

        self._raw["firmware"] = {
            "current_version": firmware_info.get(
                "current_version",
                "Inconnue",
            ),
            "latest_version": firmware_info.get(
                "latest_version",
                "Inconnue",
            ),
            "upgrade_available": firmware_info.get(
                "upgrade_available",
                False,
            ),
            "firmware_notes": firmware_info.get(
                "firmware_notes",
                [],
            ),
            "last_check": (
                datetime.now().isoformat()
            ),
        }

        return firmware_info

    async def async_get_firmware_info(
        self,
    ) -> dict:
        """Retourner les informations firmware mémorisées."""
        return await (
            self.firmware_manager.get_firmware_info()
        )

    check_firmware_upgrade = (
        async_check_firmware_upgrade
    )

    get_firmware_info = (
        async_get_firmware_info
    )

    # ------------------------------------------------------------------
    # Boucle REST
    # ------------------------------------------------------------------

    @property
    def _rest_interval(self) -> int:
        """Intervalle REST configurable."""
        try:
            return int(
                self.config_entry.options.get(
                    "rest_interval",
                    REST_INTERVAL,
                )
            )

        except (
            TypeError,
            ValueError,
        ):
            return REST_INTERVAL

    async def _rest_loop(self) -> None:
        """Interroger périodiquement l'API REST."""
        firmware_counter = 0

        delay = self._rest_interval
        failures = 0

        while True:
            # ----------------------------------------------------------
            # Appel REST.
            # ----------------------------------------------------------

            try:
                scene_data = (
                    await self.async_get_scene_data()
                )

                _LOGGER.debug(
                    "CONTENU REST BRUT (scene_data) : %s",
                    scene_data,
                )

            except asyncio.CancelledError:
                raise

            except ConfigEntryAuthFailed as err:
                _LOGGER.warning(
                    "Ré-authentification StorCube requise : %s",
                    err,
                )

                self.config_entry.async_start_reauth(
                    self.hass
                )

                return

            except Exception as err:  # noqa: BLE001
                failures += 1

                delay = min(
                    self._rest_interval
                    * (2**failures),
                    REST_BACKOFF_MAX,
                )

                if failures == 1:
                    _LOGGER.warning(
                        "Boucle REST en échec (%s), "
                        "nouvelle tentative dans %s s",
                        err,
                        delay,
                    )

                else:
                    _LOGGER.debug(
                        "Boucle REST toujours en échec "
                        "(%s tentatives) : %s",
                        failures,
                        err,
                    )

                await asyncio.sleep(delay)

                continue

            # ----------------------------------------------------------
            # REST OK.
            # ----------------------------------------------------------

            if failures:
                _LOGGER.info(
                    "Boucle REST rétablie après %s tentatives",
                    failures,
                )

            failures = 0
            delay = self._rest_interval

            try:
                if scene_data:

                    # --------------------------------------------------
                    # Découverte automatique de la pile.
                    # --------------------------------------------------

                    previous_devices = set(
                        self._known_devices
                    )

                    self._discover_stack_from_scene(
                        scene_data
                    )

                    new_devices = (
                        self._known_devices
                        - previous_devices
                    )

                    if new_devices:
                        _LOGGER.info(
                            "PILE STORCUBE DÉCOUVERTE : %s",
                            sorted(
                                self._known_devices
                            ),
                        )

                        _LOGGER.info(
                            "NOUVEAUX EQUIPID : %s",
                            sorted(new_devices),
                        )

                    # --------------------------------------------------
                    # Données REST du maître.
                    # --------------------------------------------------

                    equip_id = scene_data.get(
                        "equipId"
                    )

                    if equip_id:
                        equip_id = str(
                            equip_id
                        ).strip()

                        self._raw[
                            "rest_api"
                        ].setdefault(
                            equip_id,
                            {},
                        ).update(
                            {
                                "output_type": (
                                    scene_data.get(
                                        "outputType"
                                    )
                                ),
                                "reserved": (
                                    scene_data.get(
                                        "reserved"
                                    )
                                ),
                                "output_power": (
                                    scene_data.get(
                                        "outputPower"
                                    )
                                ),
                                "work_status": (
                                    scene_data.get(
                                        "workStatus"
                                    )
                                ),
                                "rg_online": (
                                    scene_data.get(
                                        "rgOnline"
                                    )
                                ),
                                "fg_online": (
                                    scene_data.get(
                                        "fgOnline"
                                    )
                                ),
                                "equip_type": (
                                    scene_data.get(
                                        "equipType"
                                    )
                                ),
                                "main_equip_online": (
                                    scene_data.get(
                                        "mainEquipOnline"
                                    )
                                ),
                                "equip_model": (
                                    scene_data.get(
                                        "equipModelCode"
                                    )
                                ),
                                "stack_equip_ids": (
                                    scene_data.get(
                                        "equipIds"
                                    )
                                ),
                                "last_update": (
                                    scene_data.get(
                                        "createTime"
                                    )
                                ),
                            }
                        )

                        self._last_rest_update = (
                            datetime.now().isoformat()
                        )

                        await self.async_request_refresh()

                    else:
                        _LOGGER.debug(
                            "Donnée REST sans equipId : %s",
                            scene_data,
                        )

                else:
                    _LOGGER.debug(
                        "Aucune donnée de scène récupérée"
                    )

                # ------------------------------------------------------
                # Firmware.
                # ------------------------------------------------------

                firmware_counter += 1

                if (
                    firmware_counter
                    >= FIRMWARE_EVERY
                ):
                    firmware_counter = 0

                    try:
                        if await (
                            self.async_check_firmware_upgrade()
                        ):
                            await self.async_request_refresh()

                    except Exception as err:  # noqa: BLE001
                        _LOGGER.debug(
                            "Vérification firmware échouée : %s",
                            err,
                        )

            except asyncio.CancelledError:
                raise

            except Exception as err:  # noqa: BLE001
                _LOGGER.exception(
                    "Erreur de traitement des données REST : %s",
                    err,
                )

            await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Boucle WebSocket
    # ------------------------------------------------------------------

    async def _websocket_loop(self) -> None:
        """Maintenir la connexion WebSocket."""
        retry = WS_RETRY_MIN

        master_equip_id = str(
            self.config_entry.data[
                CONF_DEVICE_ID
            ]
        ).strip()

        self._register_known_equip_id(
            master_equip_id,
            "configuration",
        )

        while True:
            try:
                # ------------------------------------------------------
                # Token.
                # ------------------------------------------------------

                token = (
                    await self._async_get_token()
                )

                # Le serveur attend le token dans l'URL.
                uri = f"{WS_URI}{token}"

                headers = {
                    "Authorization": token,
                    "Content-Type": "application/json",
                    "accept-language": "fr-FR",
                }

                # ------------------------------------------------------
                # Compatibilité websockets < 14 / >= 14.
                # ------------------------------------------------------

                try:
                    connection = (
                        websockets.connect(
                            uri,
                            additional_headers=headers,
                            ping_interval=15,
                            ping_timeout=5,
                        )
                    )

                except TypeError:
                    connection = (
                        websockets.connect(
                            uri,
                            extra_headers=headers,
                            ping_interval=15,
                            ping_timeout=5,
                        )
                    )

                async with connection as websocket:
                    self._ws_connection = websocket

                    _LOGGER.info(
                        "WebSocket StorCube connecté"
                    )

                    retry = WS_RETRY_MIN

                    # --------------------------------------------------
                    # Premier abonnement.
                    #
                    # Si REST a déjà découvert l'esclave, il est inclus.
                    # Sinon, le maître seul est envoyé.
                    # --------------------------------------------------

                    self._ws_devices_changed.clear()

                    await self._async_send_ws_subscribe(
                        websocket
                    )

                    # --------------------------------------------------
                    # Boucle de réception.
                    # --------------------------------------------------

                    while True:
                        receive_task = asyncio.create_task(
                            websocket.recv()
                        )

                        devices_task = asyncio.create_task(
                            self._ws_devices_changed.wait()
                        )

                        try:
                            done, pending = (
                                await asyncio.wait(
                                    {
                                        receive_task,
                                        devices_task,
                                    },
                                    timeout=WS_HEARTBEAT,
                                    return_when=(
                                        asyncio.FIRST_COMPLETED
                                    ),
                                )
                            )

                            # --------------------------------------------------
                            # Aucun événement pendant WS_HEARTBEAT.
                            # --------------------------------------------------

                            if not done:
                                _LOGGER.debug(
                                    "Silence WebSocket pendant "
                                    "%s secondes : "
                                    "réabonnement",
                                    WS_HEARTBEAT,
                                )

                                for task in pending:
                                    task.cancel()

                                await asyncio.gather(
                                    *pending,
                                    return_exceptions=True,
                                )

                                await self._async_send_ws_subscribe(
                                    websocket
                                )

                                continue

                            # --------------------------------------------------
                            # Un nouvel appareil a été découvert par REST
                            # ou WebSocket.
                            # --------------------------------------------------

                            if devices_task in done:
                                try:
                                    devices_task.result()

                                except asyncio.CancelledError:
                                    pass

                                self._ws_devices_changed.clear()

                                _LOGGER.info(
                                    "Nouvel equipId détecté : "
                                    "mise à jour abonnement WebSocket "
                                    "avec %s appareil(s)",
                                    len(
                                        self._known_devices
                                    ),
                                )

                                await self._async_send_ws_subscribe(
                                    websocket
                                )

                            # --------------------------------------------------
                            # Une trame WebSocket est arrivée.
                            # --------------------------------------------------

                            if receive_task in done:
                                try:
                                    message = (
                                        receive_task.result()
                                    )

                                except asyncio.CancelledError:
                                    raise

                                try:
                                    await (
                                        self._async_handle_ws_message(
                                            message
                                        )
                                    )

                                except asyncio.CancelledError:
                                    raise

                                except Exception as err:  # noqa: BLE001
                                    _LOGGER.debug(
                                        "Trame WebSocket ignorée : %s",
                                        err,
                                    )

                            # --------------------------------------------------
                            # Annuler les tâches encore actives.
                            # --------------------------------------------------

                            for task in pending:
                                if not task.done():
                                    task.cancel()

                            await asyncio.gather(
                                *pending,
                                return_exceptions=True,
                            )

                        except asyncio.CancelledError:
                            receive_task.cancel()
                            devices_task.cancel()

                            await asyncio.gather(
                                receive_task,
                                devices_task,
                                return_exceptions=True,
                            )

                            raise

                        except Exception:
                            receive_task.cancel()
                            devices_task.cancel()

                            await asyncio.gather(
                                receive_task,
                                devices_task,
                                return_exceptions=True,
                            )

                            raise

                    # Ne devrait normalement jamais être atteint.
                    self._ws_connection = None

            except asyncio.CancelledError:
                self._ws_connection = None
                raise

            except ConfigEntryAuthFailed as err:
                self._ws_connection = None

                _LOGGER.warning(
                    "Ré-authentification StorCube requise : %s",
                    err,
                )

                self.config_entry.async_start_reauth(
                    self.hass
                )

                return

            except Exception as err:  # noqa: BLE001
                self._ws_connection = None

                _LOGGER.debug(
                    "WebSocket interrompu (%s), "
                    "reconnexion dans %s s",
                    err,
                    retry,
                )

            await asyncio.sleep(retry)

            retry = min(
                retry * 2,
                WS_RETRY_MAX,
            )

    # ------------------------------------------------------------------
    # Extraction des batteries WebSocket
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_batteries(
        payload,
    ) -> list[dict]:
        """Extraire les batteries des trames WebSocket.

        Formes rencontrées :

          {
              "<equipId>": {
                  "totalPv1power": ...,
                  "list": [...]
              }
          }

          {
              "list": [...]
          }

          {
              "code": 200,
              "data": [...]
          }
        """
        batteries: list[dict] = []

        def collect(
            node,
            fallback_id=None,
        ):
            if isinstance(node, list):
                for item in node:
                    collect(
                        item,
                        fallback_id,
                    )

                return

            if not isinstance(node, dict):
                return

            inner = node.get("list")

            if isinstance(inner, list):

                # Les totaux appartiennent au stack complet.
                # Ils ne doivent être associés qu'au maître.
                totals = {
                    key: value
                    for key, value in node.items()
                    if (
                        key != "list"
                        and not isinstance(
                            value,
                            (dict, list),
                        )
                    )
                }

                items = [
                    item
                    for item in inner
                    if isinstance(
                        item,
                        dict,
                    )
                ]

                solo = len(items) == 1

                for item in items:
                    equip_id = (
                        item.get("equipId")
                        or fallback_id
                    )

                    is_master = (
                        solo
                        or (
                            fallback_id is not None
                            and str(equip_id)
                            == str(fallback_id)
                        )
                    )

                    merged = (
                        {
                            **totals,
                            **item,
                        }
                        if is_master
                        else dict(item)
                    )

                    if equip_id:
                        merged.setdefault(
                            "equipId",
                            equip_id,
                        )

                        batteries.append(
                            merged
                        )

                return

            if (
                node.get("equipId")
                or fallback_id
            ):
                entry = dict(node)

                if fallback_id:
                    entry.setdefault(
                        "equipId",
                        fallback_id,
                    )

                if entry.get("equipId"):
                    batteries.append(
                        entry
                    )

        if not isinstance(
            payload,
            dict,
        ):
            return batteries

        if isinstance(
            payload.get("list"),
            list,
        ):
            collect(payload)

        elif (
            payload.get("code") == 200
            and isinstance(
                payload.get("data"),
                list,
            )
        ):
            collect(
                payload["data"]
            )

        else:
            for key, value in payload.items():
                if isinstance(
                    value,
                    (dict, list),
                ):
                    collect(
                        value,
                        key
                        if str(key).isdigit()
                        else None,
                    )

        return batteries

    async def _async_handle_ws_message(
        self,
        message,
    ) -> None:
        """Traiter une trame WebSocket."""
        data = json.loads(message)

        # --------------------------------------------------------------
        # Accusé de réception / message non exploitable.
        # --------------------------------------------------------------

        if (
            not data
            or not isinstance(
                data,
                dict,
            )
        ):
            _LOGGER.debug(
                "Trame WebSocket non exploitable : %r",
                data,
            )

            return

        # --------------------------------------------------------------
        # Extraction des batteries.
        # --------------------------------------------------------------

        batteries = (
            self._extract_batteries(data)
        )

        if not batteries:
            _LOGGER.debug(
                "Trame WebSocket sans batterie : %s",
                data,
            )

            return

        updated = False

        # --------------------------------------------------------------
        # Chaque batterie du stack est traitée séparément.
        # --------------------------------------------------------------

        for battery in batteries:

            equip_id = battery.get(
                "equipId"
            )

            if equip_id is None:
                continue

            equip_id = str(
                equip_id
            ).strip()

            if not equip_id:
                continue

            # ----------------------------------------------------------
            # Découverte éventuelle par WebSocket.
            # ----------------------------------------------------------

            self._register_known_equip_id(
                equip_id,
                "WebSocket",
            )

            # ----------------------------------------------------------
            # Enregistrement HA.
            # ----------------------------------------------------------

            self._async_register_device(
                equip_id,
                battery,
            )

            # ----------------------------------------------------------
            # Valeurs principales.
            #
            # SOC vient du champ "soc" du WebSocket.
            # ----------------------------------------------------------

            values = {
                "status": battery.get(
                    "fgOnline",
                    0,
                ),
                "power": battery.get(
                    "invPower",
                    battery.get(
                        "power",
                        0,
                    ),
                ),
                "solar": battery.get(
                    "pv1power",
                    battery.get(
                        "solarPower",
                        0,
                    ),
                ),
                "capacity": battery.get(
                    "soc",
                    0,
                ),
            }

            # ----------------------------------------------------------
            # Stockage des données WebSocket.
            # ----------------------------------------------------------

            self._raw[
                "websocket"
            ][equip_id] = {
                "battery_status": (
                    values["status"]
                ),
                "battery_power": (
                    values["power"]
                ),
                "battery_solar": (
                    values["solar"]
                ),
                "battery_capacity": (
                    values["capacity"]
                ),

                # Trame complète.
                "battery_output": battery,

                # Format report.
                "battery_report": {
                    "list": [battery]
                },
            }

            updated = True

            # ----------------------------------------------------------
            # MQTT.
            # ----------------------------------------------------------

            await self._async_publish(
                equip_id,
                values,
                battery,
            )

        # --------------------------------------------------------------
        # Actualisation HA.
        # --------------------------------------------------------------

        if updated:
            self._last_ws_update = (
                datetime.now().isoformat()
            )

            await self.async_request_refresh()

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    async def _async_check_mqtt(
        self,
    ) -> bool:
        """Vérifier la disponibilité de MQTT."""
        try:
            await mqtt.async_wait_for_mqtt_client(
                self.hass
            )

        except Exception as err:  # noqa: BLE001
            _LOGGER.info(
                "MQTT indisponible, "
                "republication désactivée (%s)",
                err,
            )

            return False

        return True

    @staticmethod
    def _topics_for(
        equip_id: str,
    ) -> dict[str, str]:
        """Construire les topics MQTT."""
        return {
            "status": TOPIC_BATTERY_STATUS.format(
                device_id=equip_id
            ),
            "power": TOPIC_BATTERY_POWER.format(
                device_id=equip_id
            ),
            "solar": TOPIC_BATTERY_SOLAR.format(
                device_id=equip_id
            ),
            "capacity": TOPIC_BATTERY_CAPACITY.format(
                device_id=equip_id
            ),
            "report": TOPIC_BATTERY_REPORT.format(
                device_id=equip_id
            ),
            "output": TOPIC_OUTPUT.format(
                device_id=equip_id
            ),
            "output_power": TOPIC_OUTPUT_POWER.format(
                device_id=equip_id
            ),
            "threshold": TOPIC_THRESHOLD.format(
                device_id=equip_id
            ),
            "firmware": TOPIC_FIRMWARE.format(
                device_id=equip_id
            ),
        }

    async def _async_publish(
        self,
        equip_id: str,
        values: dict,
        battery: dict,
    ) -> None:
        """Publier les données sur MQTT."""
        if not self._mqtt_available:
            return

        topics = self._topics_for(
            equip_id
        )

        payloads = {
            topics["status"]: {
                "value": values["status"]
            },
            topics["power"]: {
                "value": values["power"]
            },
            topics["solar"]: {
                "value": values["solar"]
            },
            topics["capacity"]: {
                "value": values["capacity"]
            },
            topics["output"]: battery,
            topics["report"]: {
                "list": [battery]
            },
        }

        for topic, payload in payloads.items():
            try:
                await mqtt.async_publish(
                    self.hass,
                    topic,
                    json.dumps(payload),
                )

            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "Publication MQTT échouée "
                    "sur %s : %s",
                    topic,
                    err,
                )

                self._mqtt_available = False

                return

    # ------------------------------------------------------------------
    # Registre d'appareils Home Assistant
    # ------------------------------------------------------------------

    def _async_register_device(
        self,
        equip_id: str,
        battery: dict,
    ) -> None:
        """Enregistrer/vérifier une batterie dans HA."""
        device_registry = dr.async_get(
            self.hass
        )

        device_registry.async_get_or_create(
            config_entry_id=(
                self.config_entry.entry_id
            ),
            identifiers={
                (
                    DOMAIN,
                    equip_id,
                )
            },
            name=(
                f"Batterie StorCube {equip_id}"
            ),
            manufacturer="StorCube",
            model=battery.get(
                "equipType",
                "Inconnu",
            ),
            sw_version=battery.get(
                "version"
            ),
        )

        self._raw[
            "rest_api"
        ].setdefault(
            equip_id,
            {},
        )

        self._known_devices.add(
            equip_id
        )

        _LOGGER.debug(
            "Batterie StorCube enregistrée/vérifiée : %s",
            equip_id,
        )
