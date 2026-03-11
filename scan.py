#!/usr/bin/env python
"""
Simple utility script to scan for BLE devices. Useful for testing.
"""

import asyncio
from bleak import BleakScanner

async def scan():
    devices = await BleakScanner.discover()
    for d in devices:
        print(f"Address: {d.address}, Name: {d.name}, RSSI: {d.rssi} dBm")

asyncio.run(scan())
