from typing import Any, Iterable, Generator


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    """
    Process a stream of weather station events and generate snapshots/reset commands.
    
    This function processes two types of events:
    - "sample" events: Update temperature data for weather stations
    - "control" events: Generate snapshots or reset the system

    Raises exceptions if the message type or command is unknown
    """
    # Track temperature data for each weather station
    station_temperatures = {}
    # Keep track of the most recent timestamp across all events
    current_timestamp = None

    for message in events:
        try:
            if message["type"] == "sample":
                # Extract data from temperature sample event
                station_name = message["stationName"]
                timestamp = message["timestamp"]
                temperature = message["temperature"]

                # Update the current timestamp to the maximum seen so far
                current_timestamp = (
                    max(current_timestamp, timestamp)
                    if current_timestamp is not None
                    else timestamp
                )

                # Initialize station data if this is the first sample for this station
                if station_name not in station_temperatures:
                    station_temperatures[station_name] = {
                        "high": temperature, "low": temperature
                    }
                else:
                    # Update high and low temperatures for existing station
                    station_temperatures[station_name]["high"] = max(
                        station_temperatures[station_name]["high"], temperature
                    )
                    station_temperatures[station_name]["low"] = min(
                        station_temperatures[station_name]["low"], temperature
                    )

            elif message["type"] == "control":
                # Skip control commands if no data has been processed yet
                if current_timestamp is None:
                    continue

                if message["command"] == "snapshot":
                    # Generate a snapshot of current temperature data
                    output = {
                        "type": "snapshot",
                        "asOf": current_timestamp,
                        "stations": {
                            station: {"high": data["high"], "low": data["low"]}
                            for station, data in station_temperatures.items()
                        }
                    }
                    yield output

                elif message["command"] == "reset":
                    # Reset the system state and confirm reset
                    output = {
                        "type": "reset",
                        "asOf": current_timestamp
                    }
                    # Clear all accumulated data
                    station_temperatures = {}
                    current_timestamp = None
                    yield output

                else:
                    raise ValueError(f"Unknown command: {message['command']}")
            else:
                raise ValueError(f"Unknown message type: {message['type']}")

        except (KeyError, ValueError) as e:
            # Re-raise exceptions to maintain error context
            raise e
