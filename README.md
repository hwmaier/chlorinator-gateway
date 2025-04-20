# MQTT Gateway for Astral Pool chlorinators

## Intention

The intention of this application is to integrate an AstralPool Viron chlorinator and CTX 280 pool pump into [Home Assistant](https://www.home-assistant.io/) end [evcc](https://evcc.io/en/) so the pool pump can be automatically started and stopped based on surplus solar energy.

![Viron V25 chrlorinator](viron.jpg "Viron V25")

The [Astral Pool Viron eQuilibrium Chlorinator](https://github.com/pbutterworth/astralpool_chlorinator) integration for Home Assistant is already available and works well. However, it requires the chlorinator to be within Bluetooth range of the Home Assistant hardware, which isn’t always practical. For example, in my setup, the pool pump and chlorinator are located outside, well beyond the Bluetooth range of the Home Assistant controller inside the house. Fortunately, the area still has Wi-Fi coverage, making a Wi-Fi-based connection a viable alternative.


To make the data available to Home Assistant over Wi-Fi, I created a simple gateway that publishes the chlorinator data to Home Assistant's MQTT broker.
The gateway uses the [pychlorinator](https://github.com/pbutterworth/pychlorinator) library under the hood to communicate with the device.


## Requirements

### Chlorinators

Supported chlorinators include the older *Viron V* with Bluetooth and newer *Viron eQuilibrium* series from AstralPool. Support for the *Halo* series would be possible but needs minor code changes. Basically if you can control the chlorinator controller with the *ChlorinatorGO* smartphone app, this gateway should work.

Tested it with an *Astra Pool Viron V25* chlorinator.


### Gateway Hardware

A Raspberry PI with Wifi and BLE Bluetooth support. Tested it with a *Raspberry PI Zero W*.


## Installation

Install a recent version Raspberry PI OS and setup WiFi. Then add these Python requirements to your installation:

- `sudo apt-get install python3-bleak`
- `sudo apt-get install python3-paho-mqtt`
- `sudo pip3 install pycryptodome --break-system-packages`
- `sudo apt-get install python3-dotenv`
- `sudo apt-get install git`
- Clone this project
- Copy `.env.sample` to `.env` and edit the configuration settings to suit your local setup.
- Run `python3 ble2mqtt.py` to test


## Configuration

Copy `.env.sample` to `.env` and edit the configuration settings to suit your local setup. The access code is the same code used to connect with the *ChlorinatorGO* smartphone app and can be found in the chlorinators' maintenance menu.


## Credit

This work would not have been possible without the [pychlorinator](https://github.com/pbutterworth/pychlorinator) library and the [Astral Pool Viron eQuilibrium Chlorinator](https://github.com/pbutterworth/astralpool_chlorinator) plugin.
