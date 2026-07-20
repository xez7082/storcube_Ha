"""Capteurs pour l'intégration Storcube Battery Monitor."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    RestoreSensor,
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    UnitOfEnergy,
    UnitOfPower,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import StorCubeDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Accès aux données
#
# coordinator.data vaut {equip_id: {...}} où chaque dict combine :
#   - les champs REST      : output_power, reserved, work_status, ...
#   - les champs WebSocket : battery_status, battery_power, ...
#   - battery_output       : la trame brute complète de la batterie
# ---------------------------------------------------------------------------


def _raw(data: dict) -> dict:
    """Retourner la trame brute de la batterie."""
    raw = data.get("battery_output")
    return raw if isinstance(raw, dict) else {}


def _first(data: dict, *keys: str) -> Any:
    """Retourner la première valeur non nulle trouvée, brute puis REST."""
    raw = _raw(data)
    for key in keys:
        if raw.get(key) is not None:
            return raw[key]
        if data.get(key) is not None:
            return data[key]
    return None


def _num(value: Any) -> float | None:
    """Convertir en float, ou None si impossible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


OUTPUT_TYPE_MAP: dict[Any, str] = {
    0: "Normal",
    1: "Économique",
    2: "Performance",
    "manual": "Manuel",
    "auto": "Automatique",
    "eco": "Économique",
}

WORK_STATUS_MAP = {0: "Arrêté", 1: "En fonctionnement", 2: "En erreur"}

OPERATING_MODE_MAP = {0: "Normal", 1: "Économie", 2: "Boost", 3: "Veille"}


def _output_type(data: dict) -> Any:
    """Traduire le type de sortie, numérique ou textuel."""
    value = _first(data, "outputType", "output_type")
    if value is None:
        return None
    if isinstance(value, str):
        return OUTPUT_TYPE_MAP.get(value.lower(), value)
    return OUTPUT_TYPE_MAP.get(value, f"Mode {value}")


def _online(data: dict) -> str:
    """Déduire l'état de connexion global."""
    rg = _first(data, "rgOnline", "rg_online")
    main = _first(data, "mainEquipOnline", "main_equip_online")
    return "En ligne" if rg == 1 and main == 1 else "Hors ligne"


# ---------------------------------------------------------------------------
# Descriptions
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class StorCubeSensorDescription(SensorEntityDescription):
    """Description d'un capteur StorCube."""

    value_fn: Callable[[dict], Any]


SENSORS: tuple[StorCubeSensorDescription, ...] = (
    StorCubeSensorDescription(
        key="battery_level",
        name="Niveau batterie",
        icon="mdi:battery-high",
        native_unit_of_measurement=PERCENTAGE,
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: _num(_first(d, "soc", "battery_capacity")),
    ),
    StorCubeSensorDescription(
        key="battery_power",
        name="Puissance batterie",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: _num(_first(d, "invPower", "battery_power")),
    ),
    StorCubeSensorDescription(
        key="battery_temperature",
        name="Température batterie",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: _num(_first(d, "temp")),
    ),
    StorCubeSensorDescription(
        key="battery_capacity_wh",
        name="Capacité batterie",
        icon="mdi:battery-charging",
        native_unit_of_measurement=UnitOfEnergy.WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY_STORAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: _num(_first(d, "capacity")),
    ),
    StorCubeSensorDescription(
        key="battery_status",
        name="État batterie",
        value_fn=lambda d: (
            None
            if _first(d, "isWork") is None
            else ("online" if _first(d, "isWork") == 1 else "offline")
        ),
    ),
    StorCubeSensorDescription(
        key="battery_threshold",
        name="Seuil batterie",
        icon="mdi:battery-charging-medium",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: _num(_first(d, "reserved")),
    ),
    StorCubeSensorDescription(
        key="reserved",
        name="Niveau de réserve",
        icon="mdi:battery-charging-medium",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: _num(_first(d, "reserved")),
    ),
    StorCubeSensorDescription(
        key="solar_power",
        name="Puissance solaire 1",
        icon="mdi:solar-power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: _num(_first(d, "pv1power", "totalPv1power")),
    ),
    StorCubeSensorDescription(
        key="solar_power_2",
        name="Puissance solaire 2",
        icon="mdi:solar-power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: _num(_first(d, "pv2power", "totalPv2power")),
    ),
    StorCubeSensorDescription(
        key="output_power",
        name="Puissance de sortie",
        icon="mdi:flash",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: _num(
            _first(d, "outputPower", "output_power", "totalInvPower", "invPower")
        ),
    ),
    StorCubeSensorDescription(
        key="model",
        name="Modèle",
        icon="mdi:information",
        value_fn=lambda d: _first(d, "equipModelCode", "equip_model"),
    ),
    StorCubeSensorDescription(
        key="serial_number",
        name="Numéro de série",
        icon="mdi:barcode",
        value_fn=lambda d: _first(d, "equipId"),
    ),
    StorCubeSensorDescription(
        key="output_type",
        name="Type de sortie",
        icon="mdi:power-plug",
        value_fn=_output_type,
    ),
    StorCubeSensorDescription(
        key="work_status",
        name="État de fonctionnement",
        icon="mdi:power",
        value_fn=lambda d: WORK_STATUS_MAP.get(
            _first(d, "workStatus", "work_status"), "Inconnu"
        ),
    ),
    StorCubeSensorDescription(
        key="online_status",
        name="État de connexion",
        icon="mdi:wifi",
        value_fn=_online,
    ),
    StorCubeSensorDescription(
        key="error_code",
        name="Code d'erreur",
        icon="mdi:alert-circle",
        value_fn=lambda d: _first(d, "errorCode"),
    ),
    StorCubeSensorDescription(
        key="operating_mode",
        name="Mode de fonctionnement",
        icon="mdi:cog",
        value_fn=lambda d: OPERATING_MODE_MAP.get(
            _first(d, "operatingMode"), None
        ),
    ),
    StorCubeSensorDescription(
        key="firmware_version",
        name="Version firmware",
        icon="mdi:chip",
        value_fn=lambda d: _first(d, "version"),
    ),
)


@dataclass(frozen=True, kw_only=True)
class StorCubeEnergyDescription(SensorEntityDescription):
    """Description d'un compteur d'énergie intégré à partir d'une puissance."""

    power_fn: Callable[[dict], float | None]


ENERGY_SENSORS: tuple[StorCubeEnergyDescription, ...] = (
    StorCubeEnergyDescription(
        key="solar_energy",
        name="Énergie solaire 1",
        icon="mdi:solar-power",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=3,
        power_fn=lambda d: _num(_first(d, "pv1power", "totalPv1power")),
    ),
    StorCubeEnergyDescription(
        key="solar_energy_2",
        name="Énergie solaire 2",
        icon="mdi:solar-power",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=3,
        power_fn=lambda d: _num(_first(d, "pv2power", "totalPv2power")),
    ),
    StorCubeEnergyDescription(
        key="solar_energy_total",
        name="Énergie solaire totale",
        icon="mdi:solar-power",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=3,
        power_fn=lambda d: (
            (_num(_first(d, "pv1power", "totalPv1power")) or 0)
            + (_num(_first(d, "pv2power", "totalPv2power")) or 0)
        ),
    ),
    StorCubeEnergyDescription(
        key="output_energy",
        name="Énergie de sortie",
        icon="mdi:lightning-bolt",
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        device_class=SensorDeviceClass.ENERGY,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=3,
        power_fn=lambda d: _num(
            _first(d, "outputPower", "output_power", "totalInvPower", "invPower")
        ),
    ),
)


# ---------------------------------------------------------------------------
# Mise en place
# ---------------------------------------------------------------------------


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Configurer les capteurs depuis une entrée de configuration."""
    coordinator: StorCubeDataUpdateCoordinator = hass.data[DOMAIN][
        config_entry.entry_id
    ]

    known: set[str] = set()

    @callback
    def _async_add_new_devices() -> None:
        """Créer les entités des batteries découvertes depuis le dernier appel."""
        new: list[SensorEntity] = []
        for equip_id in coordinator.data or {}:
            if equip_id in known:
                continue
            known.add(equip_id)
            new.extend(
                StorCubeSensor(coordinator, equip_id, desc) for desc in SENSORS
            )
            new.extend(
                StorCubeEnergySensor(coordinator, equip_id, desc)
                for desc in ENERGY_SENSORS
            )
            new.append(StorCubeFirmwareSensor(coordinator, equip_id))
        if new:
            async_add_entities(new)

    _async_add_new_devices()
    config_entry.async_on_unload(
        coordinator.async_add_listener(_async_add_new_devices)
    )


# ---------------------------------------------------------------------------
# Entités
# ---------------------------------------------------------------------------


class StorCubeEntity(CoordinatorEntity[StorCubeDataUpdateCoordinator], SensorEntity):
    """Base commune aux capteurs StorCube."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: StorCubeDataUpdateCoordinator,
        equip_id: str,
        description: SensorEntityDescription,
    ) -> None:
        """Initialiser l'entité."""
        super().__init__(coordinator)
        self.entity_description = description
        self.equip_id = equip_id
        # Format historique conservé pour ne pas perdre l'historique des
        # entités existantes.
        self._attr_unique_id = f"{equip_id}_{description.key}"

    @property
    def device_info(self) -> DeviceInfo:
        """Rattacher l'entité à l'appareil batterie."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.equip_id)},
            name=f"Batterie StorCube {self.equip_id}",
            manufacturer="StorCube",
        )

    @property
    def _device_data(self) -> dict:
        """Retourner les données combinées de cette batterie."""
        return (self.coordinator.data or {}).get(self.equip_id) or {}

    @property
    def available(self) -> bool:
        """Indiquer si la batterie est présente dans les données."""
        return super().available and self.equip_id in (self.coordinator.data or {})


class StorCubeSensor(StorCubeEntity):
    """Capteur dont la valeur est lue directement dans les données."""

    entity_description: StorCubeSensorDescription

    @property
    def native_value(self) -> Any:
        """Retourner la valeur courante."""
        try:
            return self.entity_description.value_fn(self._device_data)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "Lecture impossible pour %s : %s", self.entity_description.key, err
            )
            return None


class StorCubeEnergySensor(StorCubeEntity, RestoreSensor):
    """Compteur d'énergie obtenu par intégration de la puissance.

    La valeur est restaurée au démarrage : sans cela, un redémarrage de Home
    Assistant remettrait le compteur à zéro et le tableau de bord Énergie
    interpréterait la remontée comme une nouvelle production.
    """

    entity_description: StorCubeEnergyDescription

    def __init__(
        self,
        coordinator: StorCubeDataUpdateCoordinator,
        equip_id: str,
        description: StorCubeEnergyDescription,
    ) -> None:
        """Initialiser le compteur."""
        super().__init__(coordinator, equip_id, description)
        self._total: float = 0.0
        self._last_power: float | None = None
        self._last_time: datetime | None = None

    async def async_added_to_hass(self) -> None:
        """Restaurer la valeur accumulée avant le redémarrage."""
        await super().async_added_to_hass()
        last_data = await self.async_get_last_sensor_data()
        if last_data is not None and last_data.native_value is not None:
            restored = _num(last_data.native_value)
            if restored is not None:
                self._total = restored
                _LOGGER.debug(
                    "Compteur %s restauré à %s kWh",
                    self.entity_description.key,
                    restored,
                )

    @callback
    def _handle_coordinator_update(self) -> None:
        """Intégrer la puissance courante sur l'intervalle écoulé."""
        power = self.entity_description.power_fn(self._device_data)
        now = datetime.now()

        if power is not None and power >= 0:
            if self._last_power is not None and self._last_time is not None:
                hours = (now - self._last_time).total_seconds() / 3600
                # Un intervalle aberrant (horloge modifiée, longue coupure)
                # ne doit pas injecter d'énergie fantôme.
                if 0 < hours < 1:
                    self._total += ((self._last_power + power) / 2) * hours / 1000
            self._last_power = power
            self._last_time = now

        super()._handle_coordinator_update()

    @property
    def native_value(self) -> float:
        """Retourner l'énergie cumulée en kWh."""
        return round(self._total, 4)


class StorCubeFirmwareSensor(
    CoordinatorEntity[StorCubeDataUpdateCoordinator], SensorEntity
):
    """État de mise à jour du firmware."""

    _attr_has_entity_name = True
    _attr_name = "Firmware"
    _attr_icon = "mdi:update"

    def __init__(
        self, coordinator: StorCubeDataUpdateCoordinator, equip_id: str
    ) -> None:
        """Initialiser le capteur."""
        super().__init__(coordinator)
        self.equip_id = equip_id
        self._attr_unique_id = f"{equip_id}_firmware"

    @property
    def device_info(self) -> DeviceInfo:
        """Rattacher l'entité à l'appareil batterie."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.equip_id)},
            name=f"Batterie StorCube {self.equip_id}",
            manufacturer="StorCube",
        )

    @property
    def native_value(self) -> str:
        """Résumer l'état du firmware."""
        info = self.coordinator.firmware
        if not info:
            return "Inconnue"
        if info.get("upgrade_available"):
            return f"Mise à jour disponible ({info.get('latest_version', 'Inconnue')})"
        return f"À jour ({info.get('current_version', 'Inconnue')})"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Exposer le détail du firmware."""
        info = self.coordinator.firmware or {}
        return {
            "current_version": info.get("current_version", "Inconnue"),
            "latest_version": info.get("latest_version", "Inconnue"),
            "upgrade_available": info.get("upgrade_available", False),
            "firmware_notes": info.get("firmware_notes", []),
            "last_check": info.get("last_check", "Jamais"),
        }
