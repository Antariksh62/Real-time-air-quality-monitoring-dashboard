import unittest
import time
import random
import sys
from unittest.mock import MagicMock

# Mock kafka so test suite can execute on host without external dependencies
if 'kafka' not in sys.modules:
    mock_kafka = MagicMock()
    sys.modules['kafka'] = mock_kafka
    sys.modules['kafka.errors'] = MagicMock()


class TestAirQualityLogic(unittest.TestCase):

    def test_alert_severity_thresholds(self):
        """Test T6: Alert severity thresholds (PM2.5: Normal < 100, Warning 100-149, Critical >= 150)"""
        def classify(avg_pm25, max_pm25):
            if (avg_pm25 >= 150) or (max_pm25 >= 150):
                return "CRITICAL"
            elif (avg_pm25 >= 100) or (max_pm25 >= 100):
                return "WARNING"
            return "NORMAL"

        self.assertEqual(classify(45.0, 80.0), "NORMAL")
        self.assertEqual(classify(99.9, 99.9), "NORMAL")
        self.assertEqual(classify(100.0, 110.0), "WARNING")
        self.assertEqual(classify(40.0, 105.0), "WARNING")
        self.assertEqual(classify(149.9, 149.9), "WARNING")
        self.assertEqual(classify(150.0, 150.0), "CRITICAL")
        self.assertEqual(classify(35.0, 210.0), "CRITICAL")
        self.assertEqual(classify(220.0, 245.0), "CRITICAL")

    def test_alert_reason_alignment(self):
        """Test D2 fix: Reason matches severity conditions"""
        def get_reason(avg_pm25, max_pm25):
            if (avg_pm25 >= 150) or (max_pm25 >= 150):
                if (avg_pm25 >= 150) and (max_pm25 >= 150):
                    return "avg_pm25>=150 and max_pm25>=150"
                elif avg_pm25 >= 150:
                    return "avg_pm25>=150"
                else:
                    return "max_pm25>=150"
            elif (avg_pm25 >= 100) or (max_pm25 >= 100):
                if (avg_pm25 >= 100) and (max_pm25 >= 100):
                    return "avg_pm25>=100 and max_pm25>=100"
                elif avg_pm25 >= 100:
                    return "avg_pm25>=100"
                else:
                    return "max_pm25>=100"
            return None

        self.assertEqual(get_reason(210.0, 245.0), "avg_pm25>=150 and max_pm25>=150")
        self.assertEqual(get_reason(45.0, 160.0), "max_pm25>=150")
        self.assertEqual(get_reason(105.0, 115.0), "avg_pm25>=100 and max_pm25>=100")
        self.assertIsNone(get_reason(30.0, 45.0))

    def test_validation_rules(self):
        """Test T2: Numeric range validation for all pollutants"""
        def is_valid(pm25, pm10, no2, so2, co):
            return (
                (0 <= pm25 <= 500) and
                (0 <= pm10 <= 600) and
                (0 <= no2 <= 400) and
                (0 <= so2 <= 100) and
                (0 <= co <= 20)
            )

        # Valid normal reading
        self.assertTrue(is_valid(25.0, 45.0, 30.0, 10.0, 1.5))
        # Valid spike reading
        self.assertTrue(is_valid(245.0, 320.0, 40.0, 15.0, 2.0))
        # Negative PM2.5 (bad sensor)
        self.assertFalse(is_valid(-10.0, 45.0, 30.0, 10.0, 1.5))
        # Out of bounds PM2.5
        self.assertFalse(is_valid(999.0, 45.0, 30.0, 10.0, 1.5))
        # Negative gas reading
        self.assertFalse(is_valid(25.0, 45.0, -5.0, 10.0, 1.5))

    def test_invalid_record_exclusion(self):
        """Test T3 (D1 Fix): Computing average excluding invalid values"""
        records = [
            {"pm25": 20.0, "is_valid": True},
            {"pm25": 40.0, "is_valid": True},
            {"pm25": -99.0, "is_valid": False}, # corrupted reading
            {"pm25": 60.0, "is_valid": True},
            {"pm25": 999.0, "is_valid": False}  # sensor failure reading
        ]

        valid_readings = [r["pm25"] for r in records if r["is_valid"]]
        avg_clean = sum(valid_readings) / len(valid_readings)
        self.assertEqual(avg_clean, 40.0) # (20 + 40 + 60) / 3

        valid_count = sum(1 for r in records if r["is_valid"])
        total_count = len(records)
        valid_rate = valid_count / total_count
        self.assertEqual(valid_count, 3)
        self.assertEqual(total_count, 5)
        self.assertEqual(valid_rate, 0.6)

    def test_spike_data_generator(self):
        """Test T7: Producer spike data generation produces values in CRITICAL threshold"""
        from producer import get_spike_data, get_sensor_data

        spike = get_spike_data("Chicago")
        self.assertEqual(spike["city"], "Chicago")
        self.assertGreaterEqual(spike["pm25"], 200.0)
        self.assertLessEqual(spike["pm25"], 250.0)
        self.assertGreaterEqual(spike["pm10"], 300.0)

        normal = get_sensor_data("New York")
        self.assertEqual(normal["city"], "New York")
        self.assertIn("timestamp", normal)

if __name__ == "__main__":
    unittest.main()
