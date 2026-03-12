#!/usr/bin/env python
"""
BLE to MQTT gateway for Astra Pool chlorinators.
"""
# @copyright © 2025 Henrik Maier. All rights reserved.
# SPDX-License-Identifier: MIT

import logging, os, sys, asyncio, json, threading
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from typing import Any
from bleak import BleakScanner, BleakClient, BleakError
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
sys.path.append('pychlorinator')
from pychlorinator.chlorinator import (
    UUID_SLAVE_SESSION_KEY,
    UUID_MASTER_AUTHENTICATION,
    UUID_CHLORINATOR_STATE,
    UUID_CHLORINATOR_APP_ACTION,
    encrypt_mac_key,
    decrypt_characteristic,
    encrypt_characteristic,
)
from pychlorinator.chlorinator_parsers import (
    Modes as ChlorinatorModes,
    SpeedLevels,
    ChlorinatorState,
    ChlorinatorAction,
    ChlorinatorActions,
)


_LOGGER = logging.getLogger()
logging.basicConfig(level=logging.INFO)

# Load the .env file with credentials and settings
load_dotenv()


#
# Constants
#
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
CHLORINATOR_NAME = os.getenv("CHLORINATOR_NAME", "POOL01")
CHLORINATOR_CODE = os.getenv("CHLORINATOR_CODE")
TOPIC_PATH = f"chlorinator/{CHLORINATOR_NAME.lower()}"
STATE_TOPIC = f"{TOPIC_PATH}/state"
ACTION_TOPIC = f"{TOPIC_PATH}/action"


class ActionEvent(asyncio.Event):
    """Custom event class which can carry an action payload"""
    def __init__(self):
        super().__init__()
        self._payload = None

    def set(self, payload=None):
        self._payload = payload
        super().set()

    async def wait(self):
        await super().wait()
        return self._payload

    def clear(self):
        self._payload = None
        super().clear()


def process_action(action, state):
    """Process the action if it leads to a state change. Otherwise discard it."""
    match action:
        case ChlorinatorActions.NoAction:
            return False
        case ChlorinatorActions.Off:
            if state['mode'] == ChlorinatorModes.Off:
                return False
        case ChlorinatorActions.Auto:
            if state['mode'] == ChlorinatorModes.Auto:
                return False
        case ChlorinatorActions.Manual:
            if state['mode'] == ChlorinatorModes.ManualOn:
                return False
        case ChlorinatorActions.Pool:
            if state['spa_selection'] == False:
                return False
        case ChlorinatorActions.Spa:
            if state['spa_selection'] == True:
                return False
        case ChlorinatorActions.Low:
            if state['pump_speed'] == SpeedLevels.Low:
                return False
        case ChlorinatorActions.Medium:
            if state['pump_speed'] == SpeedLevels.Medium:
                return False
        case ChlorinatorActions.High:
            if state['pump_speed'] == SpeedLevels.High:
                return False
    return action


def state_to_json(state):
    """Convert chlorintator state object into JSON for publishing and returns current state."""
    try:
        # Convert to string to validate against enum definitions to see if there are abnormalies.
        # We receive sometimes out-of-range values like 16, 31, 117, 129, 150 for mode.
        validate = str(state)
    except Exception as e:
        print(f"Invalid data received: {e}")
        return '{"error": true}'

    msg = {}
    msg['mode'] = state['mode'].value
    msg['pump_speed'] = state['pump_speed'].value
    msg['active_timer'] = state['active_timer']
    msg['info_message'] = str(state['info_message'])
    msg['ph_measurement'] = state['ph_measurement']
    msg['chlorine_control_status'] = state['chlorine_control_status'].value
    msg['chemistry_values_current'] = state['chemistry_values_current']
    msg['chemistry_values_valid'] = state['chemistry_values_valid']
    msg['time_hours'] = state['time_hours']
    msg['time_minutes'] = state['time_minutes']
    msg['time_seconds'] = state['time_seconds']
    msg['spa_selection'] = state['spa_selection']
    msg['pump_is_priming'] = state['pump_is_priming']
    msg['pump_is_operating'] = state['pump_is_operating']
    msg['cell_is_operating'] = state['cell_is_operating']
    msg['sanitising_until_next_timer_tomorrow'] = state['sanitising_until_next_timer_tomorrow']
    return json.dumps(msg, indent=2)


async def chlorinator_get_state(ble_device, access_code, action=None) -> dict[str, Any]:
    """Connect to the Chlorinator and read just the state and optionally write an action command."""
    result: dict[str, Any] = {}

    client = await establish_connection(
        BleakClientWithServiceCache,  # Use BleakClientWithServiceCache for service caching
        ble_device,
        ble_device.name or "Unknown Device",
        max_attempts=4,
    )

    try:
        session_key = await client.read_gatt_char(UUID_SLAVE_SESSION_KEY)

        mac = encrypt_mac_key(session_key, bytes(access_code, "utf_8"))
        await client.write_gatt_char(UUID_MASTER_AUTHENTICATION, mac)

        if action:
            print(f"BLE writing action: {action}")
            data = ChlorinatorAction(action).__bytes__()
            data = encrypt_characteristic(data, session_key)
            await client.write_gatt_char(UUID_CHLORINATOR_APP_ACTION, data)
            await asyncio.sleep(0.5) # Wait for state change to be applied before reading back            

        databytes = await client.read_gatt_char(UUID_CHLORINATOR_STATE)
        decrypted = decrypt_characteristic(databytes, session_key)
        result.update(vars(ChlorinatorState(decrypted)))

    finally:
        try:
            await client.disconnect()
        except EOFError:	
            # Known teardown issue: BlueZ closed D-Bus connection early
            pass
        
    return result


async def chlorinator_discover(mac_or_name):
    """Discover chlorinator device using BLE"""
    print(f"Scanning for chlorinator {mac_or_name}...")
    if ":" in mac_or_name:
        ble_device = await BleakScanner.find_device_by_address(mac_or_name, timeout=10.0)
    else:
        ble_device = await BleakScanner.find_device_by_name(mac_or_name, timeout=10.0)
    if ble_device:
        print(f"Chlorinator {ble_device.address} found and connected.")
    else:
        raise Exception(f"Could not find chlorinator with name or address {mac_or_name}")
    return ble_device


def on_mqtt_connect(client, userdata, flags, reason_code): # , properties):
    """MQTT connection callback handler"""
    if reason_code == mqtt.MQTT_ERR_SUCCESS:
        print(f"Connected to MQTT Broker {MQTT_BROKER}:{MQTT_PORT}.")
        client.subscribe(ACTION_TOPIC)
    else:
        print(f"MQTT connection failed with code {reason_code}!")


def on_mqtt_connect_fail(client, userdata):
    """MQTT connection failure callback handler"""
    print(f"MQTT connection failed!")


def on_mqtt_disconnect(client, userdata, disconnect_flags): #, reason_code, properties):
    """MQTT diconnection callback handler"""
    # print(f"MQTT disconnected with code {reason_code}!")
    print("MQTT disconnected!")


def on_mqtt_command(client, userdata, message):
    """Callback when we receive a new chlorinator action command from MQTT broker."""
    loop, event = userdata
    try:
        action = ChlorinatorActions(int(message.payload))
        loop.call_soon_threadsafe(event.set, action)
        print(f"New action: {action}")
    except:
        print(f"Unknown action: {message.payload}")


def mqtt_thread(client):
    """MQTT backgroun thread. Handles connection reties automatically."""
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_mqtt_connect
    client.on_disconnect = on_mqtt_disconnect
    client.on_connect_fail = on_mqtt_connect_fail
    client.message_callback_add(ACTION_TOPIC, on_mqtt_command)
    client.reconnect_delay_set(min_delay=10, max_delay=60)
    client.connect_async(MQTT_BROKER, MQTT_PORT, 10)
    client.loop_forever(retry_first_connection=True)


async def main():
    """Main thread"""
    loop = asyncio.get_running_loop()
    mqttEvent = ActionEvent()
    mqttClient = mqtt.Client(userdata=(loop, mqttEvent))
    bleDevice = None
    pendingAction = None
    threading.Thread(target=mqtt_thread, args=(mqttClient,), daemon=True).start()

    while True:
        if mqttClient.is_connected():
            #
            # Re-connect bluetooth
            #
            if bleDevice is None:
                try:
                    bleDevice = await chlorinator_discover(CHLORINATOR_NAME)
                except Exception as e:
                    print(f"BLE connection failed: {e}")
                    mqttClient.publish(STATE_TOPIC, state_to_json({"error": f"BLE connection failed: {e}"}))
                    await asyncio.sleep(10) # Re-connection delay
                    continue

            #
            # Read chlorinator state
            #
            try:
                state = await chlorinator_get_state(bleDevice, CHLORINATOR_CODE, pendingAction)
                pendingAction = False
                json = state_to_json(state)
                mqttClient.publish(STATE_TOPIC, json)

                #
                # Wait for next cycle, either time or a new MQTT message
                #
                try:
                    newAction = await asyncio.wait_for(mqttEvent.wait(), timeout=10)
                    mqttEvent.clear()
                    pendingAction = process_action(newAction, state)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

            except BleakError as e:
                print(f"BLE error: {e}, trying to reconnect...")
                mqttClient.publish(STATE_TOPIC, state_to_json({"error": f"BLE error: {e}"}))
                bleDevice = None  # Force rescan and reinit
                await asyncio.sleep(10)
            except Exception as e:
                print(f"Unexpected error: {e}")
                mqttClient.publish(STATE_TOPIC, state_to_json({"error": f"Unexpected error: {e}"}))

        else:
            bleDevice = None
            await asyncio.sleep(10)


#
# Run main thread 
#
asyncio.run(main())
