from . import weather


def test_empty_events():
    """Test processing empty events list"""
    assert not list(weather.process_events([]))


def test_single_sample_event():
    """Test processing a single sample event"""
    events = [{"type": "sample", "stationName": "Station1", "timestamp": 100, "temperature": 25.5}]
    result = list(weather.process_events(events))
    assert not result


def test_snapshot_with_samples():
    """Test snapshot command after sample events"""
    events = [
        {"type": "sample", "stationName": "Station1", "timestamp": 100, "temperature": 25.5},
        {"type": "sample", "stationName": "Station1", "timestamp": 200, "temperature": 30.0},
        {"type": "control", "command": "snapshot"}
    ]
    result = list(weather.process_events(events))
    assert result == [{
        "type": "snapshot",
        "asOf": 200,
        "stations": {"Station1": {"high": 30.0, "low": 25.5}}
    }]


def test_multiple_stations():
    """Test multiple weather stations"""
    events = [
        {"type": "sample", "stationName": "Station1", "timestamp": 100, "temperature": 25.5},
        {"type": "sample", "stationName": "Station2", "timestamp": 150, "temperature": 20.0},
        {"type": "sample", "stationName": "Station1", "timestamp": 200, "temperature": 30.0},
        {"type": "control", "command": "snapshot"}
    ]
    result = list(weather.process_events(events))
    assert result == [{
        "type": "snapshot",
        "asOf": 200,
        "stations": {
            "Station1": {"high": 30.0, "low": 25.5},
            "Station2": {"high": 20.0, "low": 20.0}
        }
    }]


def test_reset_command():
    """Test reset command clears state"""
    events = [
        {"type": "sample", "stationName": "Station1", "timestamp": 100, "temperature": 25.5},
        {"type": "control", "command": "snapshot"},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "Station1", "timestamp": 300, "temperature": 15.0},
        {"type": "control", "command": "snapshot"}
    ]
    result = list(weather.process_events(events))
    assert result == [
        {
            "type": "snapshot",
            "asOf": 100,
            "stations": {"Station1": {"high": 25.5, "low": 25.5}}
        },
        {
            "type": "reset",
            "asOf": 100
        },
        {
            "type": "snapshot",
            "asOf": 300,
            "stations": {"Station1": {"high": 15.0, "low": 15.0}}
        }
    ]


def test_snapshot_without_samples():
    """Test snapshot command without any sample events"""
    events = [{"type": "control", "command": "snapshot"}]
    result = list(weather.process_events(events))
    assert not result


def test_invalid_message_type():
    """Test handling of invalid message type"""
    events = [{"type": "invalid", "data": "test"}]
    try:
        list(weather.process_events(events))
        assert False, "Expected ValueError but no exception was raised"
    except ValueError as e:
        assert str(e) == "Unknown message type: invalid"


def test_missing_fields():
    """Test handling of messages with missing fields"""
    events = [{"type": "sample"}]
    try:
        list(weather.process_events(events))
        assert False, "Expected KeyError but no exception was raised"
    except KeyError:
        pass


def test_timestamp_ordering():
    """Test that current_timestamp uses max timestamp"""
    events = [
        {"type": "sample", "stationName": "Station1", "timestamp": 300, "temperature": 25.5},
        {"type": "sample", "stationName": "Station1", "timestamp": 100, "temperature": 30.0},
        {"type": "control", "command": "snapshot"}
    ]
    result = list(weather.process_events(events))
    assert result == [{
        "type": "snapshot",
        "asOf": 300,
        "stations": {"Station1": {"high": 30.0, "low": 25.5}}
    }]


def test_complex_scenario():
    """Test complex scenario with all message types and commands"""
    events = [
        {"type": "sample", "stationName": "Station1", "timestamp": 100, "temperature": 25.5},
        {"type": "sample", "stationName": "Station1", "timestamp": 125, "temperature": 21.0},
        {"type": "sample", "stationName": "Station2", "timestamp": 150, "temperature": 20.0},
        {"type": "sample", "stationName": "Station3", "timestamp": 200, "temperature": 30.0},
        {"type": "control", "command": "snapshot"},
        {"type": "sample", "stationName": "Station1", "timestamp": 300, "temperature": 35.0},
        {"type": "sample", "stationName": "Station2", "timestamp": 350, "temperature": 15.0},
        {"type": "sample", "stationName": "Station4", "timestamp": 400, "temperature": 22.5},
        {"type": "control", "command": "snapshot"},
        {"type": "control", "command": "reset"},
        {"type": "sample", "stationName": "Station1", "timestamp": 500, "temperature": 10.0},
        {"type": "sample", "stationName": "Station5", "timestamp": 550, "temperature": 40.0},
        {"type": "control", "command": "snapshot"}
    ]

    result = list(weather.process_events(events))

    assert result == [
        {
            "type": "snapshot",
            "asOf": 200,
            "stations": {
                "Station1": {"high": 25.5, "low": 21.0},
                "Station2": {"high": 20.0, "low": 20.0},
                "Station3": {"high": 30.0, "low": 30.0}
            }
        },
        {
            "type": "snapshot",
            "asOf": 400,
            "stations": {
                "Station1": {"high": 35.0, "low": 21.0},
                "Station2": {"high": 20.0, "low": 15.0},
                "Station3": {"high": 30.0, "low": 30.0},
                "Station4": {"high": 22.5, "low": 22.5}
            }
        },
        {
            "type": "reset",
            "asOf": 400
        },
        {
            "type": "snapshot",
            "asOf": 550,
            "stations": {
                "Station1": {"high": 10.0, "low": 10.0},
                "Station5": {"high": 40.0, "low": 40.0}
            }
        }
    ]
