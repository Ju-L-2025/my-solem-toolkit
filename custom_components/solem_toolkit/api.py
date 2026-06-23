"""Solem BLE API helper.

This is a lightweight subset of the Solem API used by the scheduling integration.
It focuses on robust BLE connection handling and command writes for manual actions.
"""

from __future__ import annotations

import asyncio
import logging
import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakDBusError
from bleak_retry_connector import (
    BleakOutOfConnectionSlotsError,
    establish_connection,
)
from tenacity import retry, stop_after_attempt, wait_exponential

from homeassistant.components.bluetooth import async_ble_device_from_address
from homeassistant.core import HomeAssistant

from .const import CHARACTERISTIC_UUID, DEFAULT_BLUETOOTH_TIMEOUT, NOTIFICATION_UUID

_LOGGER = logging.getLogger(__name__)

# Module-level lock shared by ALL SolemAPI instances.
# This is critical because services.py creates a new SolemAPI per service call,
# so a per-instance lock would be useless for preventing concurrent BLE access.
_GLOBAL_BLE_LOCK = asyncio.Lock()

# Cooldown between BLE sessions (seconds). The Solem controller needs time
# to fully release a connection before accepting a new one.
_BLE_COOLDOWN_SECONDS = 3.0


class APIConnectionError(Exception):
    """Exception raised when a BLE connection or write fails."""


class SolemAPI:
    """API wrapper for the Solem BLE protocol."""

    def __init__(
        self,
        hass: HomeAssistant,
        mac_address: str,
        bluetooth_timeout: int = DEFAULT_BLUETOOTH_TIMEOUT,
    ) -> None:
        self.hass = hass
        self.mac_address = mac_address
        self.bluetooth_timeout = bluetooth_timeout

        self.characteristic_uuid: str = CHARACTERISTIC_UUID

    async def scan_bluetooth(self) -> list[BLEDevice]:
        """Return a list of discovered BLE devices."""
        return await BleakScanner.discover(timeout=5.0)

    async def _find_device(self) -> BLEDevice:
        """Find the device by MAC address."""
        if not self.controller_mac:
            _LOGGER.error("BLE_FATAL_ERROR: MAC address not provided to _find_device")
            raise APIConnectionError("MAC address not provided")

        device = bluetooth.async_ble_device_from_address(
            self.hass, self.controller_mac, connectable=True
        )
        if device:
            return device

        devices = await self.scan_bluetooth()
        for d in devices:
            if (d.address or "").lower() == self.controller_mac.lower():
                return d

        _LOGGER.error("BLE_FATAL_ERROR: Device %s not found in Bluetooth scan! Is it turned on and in range?", self.controller_mac)
        raise APIConnectionError(f"Device {self.controller_mac} not found in scan")

    async def _connect_client(self) -> BleakClient:
        """Establish a robust connection using bleak-retry-connector."""
        ble_device = await self._find_device()
        try:
            client = await establish_connection(
                BleakClient,
                ble_device,
                name=f"Solem - {self.mac_address}",
                timeout=self.bluetooth_timeout,
                max_attempts=3,
            )
            return client
        except BleakOutOfConnectionSlotsError as exc:
            _LOGGER.error("BLE_FATAL_ERROR: Bluetooth adapter/proxy out of connection slots for %s: %s", self.controller_mac, repr(exc))
            raise APIConnectionError(
                "Bluetooth adapter/proxy out of connection slots or device busy/unreachable"
            ) from exc
        except (BleakDBusError, TimeoutError, OSError) as exc:
            _LOGGER.error("BLE_FATAL_ERROR: Timeout/DBus error connecting to %s: %s", self.controller_mac, repr(exc))
            raise APIConnectionError("Timeout connecting to device") from exc
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("BLE_FATAL_ERROR: Unexpected connection error for %s: %s", self.controller_mac, repr(exc))
            raise APIConnectionError("Unexpected BLE connection error") from exc

    async def list_characteristics(self) -> dict:
        """Return discovered services/characteristics (debug helper)."""
        client = await self._connect_client()
        try:
            if not client.is_connected:
                raise APIConnectionError("Failed connecting!")

            # Home Assistant wraps BleakClient (HaBleakClientWrapper) and does not
            # expose BleakClient.get_services(). After connecting, discovered
            # services are available via the `services` attribute.
            services = getattr(client, "services", None)
            if services is None:
                # Last-resort fallback for non-HA clients / unexpected wrappers.
                inner = getattr(client, "_client", None) or getattr(client, "_bleak_client", None)
                if inner is not None and hasattr(inner, "get_services"):
                    services = await inner.get_services()
                else:
                    raise APIConnectionError("Services not available on this platform/client")
            result: dict = {}
            for svc in services:
                chars = []
                for c in svc.characteristics:
                    chars.append(
                        {
                            "uuid": str(c.uuid),
                            "properties": list(c.properties),
                            "descriptors": [str(d.uuid) for d in c.descriptors],
                        }
                    )
                result[str(svc.uuid)] = chars
            return result
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.4, min=0.4, max=2))
    async def _write_with_auth_retry(self, client: BleakClient, payload: bytes) -> None:
        """Write with a small retry loop (Solem can be picky right after connect)."""
        if not client.is_connected:
            raise APIConnectionError("Client not connected")

        await client.write_gatt_char(self.characteristic_uuid, payload, response=False)

    @asynccontextmanager
    async def _notification_session(self, client: BleakClient, event: asyncio.Event | None = None) -> AsyncIterator[None]:
        """Subscribe to controller notifications for the duration of a command."""
        def callback(sender, data: bytearray) -> None:
            _LOGGER.debug("Notification from %s: %s", sender, data.hex())
            if event is not None:
                event.set()

        try:
            await client.start_notify(NOTIFICATION_UUID, callback)
        except Exception as exc:  # noqa: BLE001
            raise APIConnectionError(
                "Failed subscribing to controller notifications"
            ) from exc

        try:
            yield
        finally:
            with suppress(Exception):
                await client.stop_notify(NOTIFICATION_UUID)

    async def _write_and_commit(self, command: bytes) -> None:
        """Write a command then commit it (Solem protocol) - Ultimate Edition."""
        async with _GLOBAL_BLE_LOCK:
            client = await self._connect_client()
            try:
                if not client.is_connected:
                    raise APIConnectionError("Failed connecting!")

                notify_event = asyncio.Event()

                # 1. We subscribe to the Notification channel to stabilize the BLE stack
                async with self._notification_session(client, notify_event):
                    # Tiny wait to allow the notification subscription to be fully active
                    await asyncio.sleep(0.5)
                    
                    # 2. We send the command
                    await self._write_with_auth_retry(client, command)
                    
                    # 3. WE WAIT for the Solem to acknowledge the command via notification
                    try:
                        await asyncio.wait_for(notify_event.wait(), timeout=3.0)
                    except TimeoutError:
                        _LOGGER.error("BLE_TIMEOUT_ERROR: Timeout waiting for Solem command ACK notification. Did the controller crash?")
                        
                    notify_event.clear()
                    
                    # 4. We send the commit frame
                    commit = struct.pack(">BB", 0x3B, 0x00)
                    await self._write_with_auth_retry(client, commit)
                    
                    # 5. WE WAIT for the Solem to send the final 18-byte status notification
                    try:
                        await asyncio.wait_for(notify_event.wait(), timeout=4.0)
                    except TimeoutError:
                        _LOGGER.error("BLE_TIMEOUT_ERROR: Timeout waiting for Solem final 18-byte STATUS notification. Disconnecting prematurely!")
                        
                    # Tiny grace period so the Bluetooth stack digests the final packet
                    await asyncio.sleep(0.5)
                    
            finally:
                with suppress(Exception):
                    await client.disconnect()
                # 6. Cooldown: let the Solem controller fully release the BLE session
                # before allowing the next command through the global lock
                await asyncio.sleep(_BLE_COOLDOWN_SECONDS)

    async def turn_on(self) -> None:
        """Turn on controller (enable watering)."""
        command = struct.pack(">HBBBH", 0x3105, 0xA0, 0x00, 0x01, 0x0000)
        await self._write_and_commit(command)

    async def turn_off_permanent(self) -> None:
        """Disable watering permanently."""
        command = struct.pack(">HBBBH", 0x3105, 0xC0, 0x00, 0x00, 0x0000)
        await self._write_and_commit(command)

    async def turn_off_x_days(self, days: int) -> None:
        """Disable watering for X days."""
        days = max(0, min(days, 15))
        command = struct.pack(">HBBBH", 0x3105, 0xC0, 0x00, days, 0x0000)
        await self._write_and_commit(command)

    async def sprinkle_station_x_for_y_minutes(self, station: int, minutes: int) -> None:
        """Manually water a station for Y minutes."""
        station = max(1, min(station, 16))
        minutes = max(1, min(minutes, 720))
        seconds = minutes * 60
        command = struct.pack(">HBBBH", 0x3105, 0x12, station, 0x00, seconds)
        await self._write_and_commit(command)

    async def sprinkle_all_stations_for_y_minutes(self, minutes: int) -> None:
        """Manually water all stations for Y minutes each."""
        minutes = max(1, min(minutes, 720))
        seconds = minutes * 60
        command = struct.pack(">HBBBH", 0x3105, 0x11, 0x00, 0x00, seconds)
        await self._write_and_commit(command)

    async def run_program_x(self, program: int) -> None:
        """Run a controller program by id (1-3 on most devices)."""
        program = max(1, min(program, 3))
        command = struct.pack(">HBBBH", 0x3105, 0x14, 0x00, program, 0x0000)
        await self._write_and_commit(command)

    async def stop_manual_sprinkle(self) -> None:
        """Stop any running manual watering session."""
        command = struct.pack(">HBBBH", 0x3105, 0x15, 0x00, 0xFF, 0x0000)
        await self._write_and_commit(command)
