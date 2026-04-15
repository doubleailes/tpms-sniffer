use serde::{Deserialize, Serialize};

/// Vehicle class inferred from tire pressure range and sensor count.
///
/// Classification uses pressure as the primary signal and sensor count as a
/// secondary signal where available.  Overlapping pressure ranges (e.g.
/// 260–280 kPa is valid for both passenger cars and SUVs) are resolved in
/// favour of the lower class until sensor count data is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VehicleClass {
    Motorcycle,         // 2 sensors, 200–290 kPa
    PassengerCar,       // 4 sensors, 200–280 kPa
    SuvLightTruck,      // 4 sensors, 260–340 kPa
    LightCommercialVan, // 4+ sensors, 350–480 kPa
    HeavyTruck,         // 6–18 sensors, 550–800 kPa
    Unknown,            // insufficient data or ambiguous range
}

impl VehicleClass {
    /// Expected number of TPMS sensors for this class.
    /// Used by the Jaccard grouper to set the target group size.
    ///
    /// `HeavyTruck` returns `None` because heavy trucks have 6–18 sensors
    /// depending on axle count, which is too variable to fix.
    pub fn expected_sensor_count(&self) -> Option<usize> {
        match self {
            Self::Motorcycle => Some(2),
            Self::PassengerCar => Some(4),
            Self::SuvLightTruck => Some(4),
            Self::LightCommercialVan => Some(4), // sometimes more on long-wheelbase
            Self::HeavyTruck => None,            // 6–18, too variable to fix
            Self::Unknown => None,
        }
    }

    /// Pressure tolerance to use when fingerprint-matching vehicles of this
    /// class.  Heavier vehicles have larger absolute pressure variation under
    /// load and temperature change, so they need a wider tolerance.
    pub fn pressure_tolerance_kpa(&self) -> f32 {
        match self {
            Self::Motorcycle => 4.0,
            Self::PassengerCar => 5.0,
            Self::SuvLightTruck => 6.0,
            Self::LightCommercialVan => 8.0,
            Self::HeavyTruck => 15.0,
            Self::Unknown => 5.0,
        }
    }

    /// Parse from a database string.
    pub fn from_str(s: &str) -> Self {
        match s {
            "Motorcycle" => Self::Motorcycle,
            "PassengerCar" => Self::PassengerCar,
            "SuvLightTruck" => Self::SuvLightTruck,
            "LightCommercialVan" => Self::LightCommercialVan,
            "HeavyTruck" => Self::HeavyTruck,
            _ => Self::Unknown,
        }
    }

    /// Return the canonical string label for database persistence.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Motorcycle => "Motorcycle",
            Self::PassengerCar => "PassengerCar",
            Self::SuvLightTruck => "SuvLightTruck",
            Self::LightCommercialVan => "LightCommercialVan",
            Self::HeavyTruck => "HeavyTruck",
            Self::Unknown => "Unknown",
        }
    }
}

impl std::fmt::Display for VehicleClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Infer the vehicle class from pressure (kPa) and an optional sensor count.
///
/// Classification uses pressure as the primary signal and sensor count as a
/// secondary signal where available.  See the issue description for the
/// boundary table.
pub fn infer_vehicle_class(pressure_kpa: f32, sensor_count: Option<usize>) -> VehicleClass {
    let p = pressure_kpa as u32;

    // Unambiguous ranges — sensor count not needed
    if p >= 550 {
        return VehicleClass::HeavyTruck;
    }
    if (350..550).contains(&p) {
        return VehicleClass::LightCommercialVan;
    }

    // Ambiguous ranges — use sensor count to disambiguate
    if (200..350).contains(&p) {
        return match sensor_count {
            Some(2) => VehicleClass::Motorcycle,
            Some(4) => {
                if p >= 260 {
                    VehicleClass::SuvLightTruck
                } else {
                    VehicleClass::PassengerCar
                }
            }
            Some(n) if n > 4 => VehicleClass::HeavyTruck,
            _ => {
                // No sensor count yet — classify conservatively
                if p >= 260 {
                    VehicleClass::SuvLightTruck
                } else {
                    VehicleClass::PassengerCar
                }
            }
        };
    }

    VehicleClass::Unknown
}

// ---------------------------------------------------------------------------
// Temperature-pressure compensation
// ---------------------------------------------------------------------------

/// Approximate pressure increase per °C of temperature rise above the cold
/// inflation baseline.  Based on the ideal gas law approximation for tire
/// pressure temperature dependence.
const KPA_PER_DEGREE_C: f32 = 0.9;

/// Standard cold inflation reference temperature (°C), per automotive industry
/// conventions.  Most tire pressure specifications assume cold inflation at
/// approximately 20 °C ambient.
const COLD_TEMP_C: f32 = 20.0;

/// Adjust a raw pressure reading back to its cold-equivalent value based on
/// the sensor temperature.  Only applied when the temperature is within a
/// plausible range (0–100 °C); sentinel values (e.g. 215 °C) or negative
/// readings are left uncompensated.
pub fn compensate_pressure(pressure_kpa: f32, temp_c: Option<f32>) -> f32 {
    match temp_c {
        Some(t) if t > 0.0 && t < 100.0 => {
            // Adjust back to cold-equivalent pressure
            pressure_kpa - (t - COLD_TEMP_C) * KPA_PER_DEGREE_C
        }
        _ => pressure_kpa, // sentinel or unavailable — no compensation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------
    // infer_vehicle_class — boundary values from acceptance criteria
    // -------------------------------------------------------------------

    #[test]
    fn boundary_200_kpa_no_count() {
        assert_eq!(
            infer_vehicle_class(200.0, None),
            VehicleClass::PassengerCar
        );
    }

    #[test]
    fn boundary_260_kpa_no_count() {
        assert_eq!(
            infer_vehicle_class(260.0, None),
            VehicleClass::SuvLightTruck
        );
    }

    #[test]
    fn boundary_290_kpa_no_count() {
        assert_eq!(
            infer_vehicle_class(290.0, None),
            VehicleClass::SuvLightTruck
        );
    }

    #[test]
    fn boundary_340_kpa_no_count() {
        assert_eq!(
            infer_vehicle_class(340.0, None),
            VehicleClass::SuvLightTruck
        );
    }

    #[test]
    fn boundary_350_kpa() {
        assert_eq!(
            infer_vehicle_class(350.0, None),
            VehicleClass::LightCommercialVan
        );
    }

    #[test]
    fn boundary_480_kpa() {
        assert_eq!(
            infer_vehicle_class(480.0, None),
            VehicleClass::LightCommercialVan
        );
    }

    #[test]
    fn boundary_550_kpa() {
        assert_eq!(infer_vehicle_class(550.0, None), VehicleClass::HeavyTruck);
    }

    // -------------------------------------------------------------------
    // Sensor count disambiguation
    // -------------------------------------------------------------------

    #[test]
    fn motorcycle_2_sensors_at_250() {
        assert_eq!(
            infer_vehicle_class(250.0, Some(2)),
            VehicleClass::Motorcycle
        );
    }

    #[test]
    fn passenger_car_4_sensors_at_254() {
        assert_eq!(
            infer_vehicle_class(254.0, Some(4)),
            VehicleClass::PassengerCar
        );
    }

    #[test]
    fn suv_4_sensors_at_300() {
        assert_eq!(
            infer_vehicle_class(300.0, Some(4)),
            VehicleClass::SuvLightTruck
        );
    }

    #[test]
    fn heavy_truck_more_than_4_sensors() {
        assert_eq!(
            infer_vehicle_class(280.0, Some(6)),
            VehicleClass::HeavyTruck
        );
    }

    // -------------------------------------------------------------------
    // Real-world acceptance criteria
    // -------------------------------------------------------------------

    #[test]
    fn ave_tpms_382_kpa_is_light_commercial_van() {
        assert_eq!(
            infer_vehicle_class(382.0, None),
            VehicleClass::LightCommercialVan
        );
    }

    #[test]
    fn hyundai_elantra_254_kpa_is_passenger_car() {
        // Hyundai Elantra at ~254 kPa classified as PassengerCar
        assert_eq!(
            infer_vehicle_class(254.0, None),
            VehicleClass::PassengerCar
        );
    }

    #[test]
    fn trw_ook_63_kpa_is_unknown() {
        // TRW-OOK at ~63 kPa classified as Unknown (below valid range)
        assert_eq!(infer_vehicle_class(63.0, None), VehicleClass::Unknown);
    }

    // -------------------------------------------------------------------
    // expected_sensor_count
    // -------------------------------------------------------------------

    #[test]
    fn expected_sensor_count_values() {
        assert_eq!(VehicleClass::Motorcycle.expected_sensor_count(), Some(2));
        assert_eq!(VehicleClass::PassengerCar.expected_sensor_count(), Some(4));
        assert_eq!(VehicleClass::SuvLightTruck.expected_sensor_count(), Some(4));
        assert_eq!(
            VehicleClass::LightCommercialVan.expected_sensor_count(),
            Some(4)
        );
        assert_eq!(VehicleClass::HeavyTruck.expected_sensor_count(), None);
        assert_eq!(VehicleClass::Unknown.expected_sensor_count(), None);
    }

    // -------------------------------------------------------------------
    // pressure_tolerance_kpa
    // -------------------------------------------------------------------

    #[test]
    fn pressure_tolerance_values() {
        assert_eq!(VehicleClass::Motorcycle.pressure_tolerance_kpa(), 4.0);
        assert_eq!(VehicleClass::PassengerCar.pressure_tolerance_kpa(), 5.0);
        assert_eq!(VehicleClass::SuvLightTruck.pressure_tolerance_kpa(), 6.0);
        assert_eq!(
            VehicleClass::LightCommercialVan.pressure_tolerance_kpa(),
            8.0
        );
        assert_eq!(VehicleClass::HeavyTruck.pressure_tolerance_kpa(), 15.0);
        assert_eq!(VehicleClass::Unknown.pressure_tolerance_kpa(), 5.0);
    }

    // -------------------------------------------------------------------
    // Temperature-pressure compensation
    // -------------------------------------------------------------------

    #[test]
    fn compensate_at_reference_temp_unchanged() {
        let p = compensate_pressure(230.0, Some(20.0));
        assert!((p - 230.0).abs() < 0.01);
    }

    #[test]
    fn compensate_at_higher_temp_reduces_pressure() {
        // 50 °C above reference → −27 kPa compensation
        let p = compensate_pressure(257.0, Some(50.0));
        assert!((p - 230.0).abs() < 0.01);
    }

    #[test]
    fn compensate_at_lower_temp_increases_pressure() {
        // 10 °C below reference → +9 kPa compensation
        let p = compensate_pressure(221.0, Some(10.0));
        assert!((p - 230.0).abs() < 0.01);
    }

    #[test]
    fn compensate_sentinel_temp_no_change() {
        // Sentinel (215 °C) — no compensation
        let p = compensate_pressure(230.0, Some(215.0));
        assert!((p - 230.0).abs() < 0.01);
    }

    #[test]
    fn compensate_negative_temp_no_change() {
        let p = compensate_pressure(230.0, Some(-10.0));
        assert!((p - 230.0).abs() < 0.01);
    }

    #[test]
    fn compensate_none_temp_no_change() {
        let p = compensate_pressure(230.0, None);
        assert!((p - 230.0).abs() < 0.01);
    }

    // -------------------------------------------------------------------
    // Round-trip string conversion
    // -------------------------------------------------------------------

    #[test]
    fn vehicle_class_round_trip() {
        for class in &[
            VehicleClass::Motorcycle,
            VehicleClass::PassengerCar,
            VehicleClass::SuvLightTruck,
            VehicleClass::LightCommercialVan,
            VehicleClass::HeavyTruck,
            VehicleClass::Unknown,
        ] {
            assert_eq!(VehicleClass::from_str(class.as_str()), *class);
        }
    }
}
