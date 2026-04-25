/*
 * MLX90394 multi-TCA round-robin reader (low-RAM version)
 *
 * Intended for small-SRAM boards such as ATmega328P.
 *
 * Topology:
 *   - Root TCA9548A at 0x70
 *   - Downstream TCA9548A at 0x77 behind root channels 0..6
 *   - Up to 2x MLX90394 per downstream channel at 0x60 and 0x61
 *
 * Behavior:
 *   - Scans root channels 0..6 for second-stage TCA at 0x77
 *   - Scans all second-stage channels 0..7 for MLX90394 at 0x60 / 0x61
 *   - Configures detected sensors for CONFIG=2, max filtering, OSR enabled,
 *     continuous mode at 5 Hz
 *   - Polls sensors round-robin and caches latest sample
 *   - Prints a formatted snapshot every 2 s (0.5 Hz)
 *
 * Commands:
 *   - scan
 *   - stop
 *   - start
 */

#include <Wire.h>
#include <string.h>
#include <stdlib.h>

// -----------------------------------------------------------------------------
// MLX90394 registers
// -----------------------------------------------------------------------------
static const uint8_t REG_STAT1      = 0x00;
static const uint8_t REG_STAT2      = 0x07;
static const uint8_t REG_CTRL1      = 0x0E;
static const uint8_t REG_CTRL2      = 0x0F;
static const uint8_t REG_CTRL3      = 0x14;
static const uint8_t REG_CTRL4      = 0x15;
static const uint8_t REG_COMPANY_ID = 0x0A;

static const uint8_t DRDY_BIT  = 0x01;
static const uint8_t STAT2_XOV = 0x01;
static const uint8_t STAT2_YOV = 0x02;
static const uint8_t STAT2_ZOV = 0x04;
static const uint8_t STAT2_DOR = 0x08;

static const uint8_t CTRL1_X_EN = 0x10;
static const uint8_t CTRL1_Y_EN = 0x20;
static const uint8_t CTRL1_Z_EN = 0x40;
static const uint8_t CTRL1_XYZ_EN = CTRL1_X_EN | CTRL1_Y_EN | CTRL1_Z_EN;
static const uint8_t MODE_POWER_DOWN = 0x00;
static const uint8_t MODE_CONT_5HZ   = 0x02;

static const uint8_t CONFIG_LOW_NOISE_HIGH_SENS = 0x02;
static const uint8_t OSR_ENABLED = 0x01;
static const uint8_t DIG_FILT_MAX = 0x07;
static const uint8_t CTRL4_T_EN = 0x20;

static const float SENS_UT_PER_LSB = 0.15f;
static const float TEMP_LSB_PER_C  = 50.0f;

// -----------------------------------------------------------------------------
// Topology / scan settings
// -----------------------------------------------------------------------------
static const uint8_t TCA1_ADDR = 0x70;
static const uint8_t TCA2_ADDR = 0x77;
static const uint8_t TCA1_CH_COUNT = 7;
static const uint8_t TCA2_CH_COUNT = 8;

static const uint8_t MLX_ADDR_A = 0x60;
static const uint8_t MLX_ADDR_B = 0x61;
static const uint8_t MLX_PER_CHANNEL = 2;
static const uint8_t SENSOR_COUNT = TCA1_CH_COUNT * TCA2_CH_COUNT * MLX_PER_CHANNEL; // 112

static const uint32_t SNAPSHOT_PERIOD_MS = 500UL;   // was 2000UL

// -----------------------------------------------------------------------------
// State / low-RAM storage
// -----------------------------------------------------------------------------
static uint8_t g_last_i2c_error = 0;
static uint8_t g_tca2_present_mask = 0; // bit per TCA1 channel
static bool g_streaming_enabled = false;
static uint32_t g_last_snapshot_ms = 0;
static uint8_t g_next_sensor_index = 0;
static uint8_t g_present_sensor_count = 0;
static uint8_t g_configured_sensor_count = 0;

static const uint8_t F_PRESENT    = 0x01;
static const uint8_t F_CONFIGURED = 0x02;
static const uint8_t F_HAVE       = 0x04;
static const uint8_t F_LAST_OK    = 0x08;
static const uint8_t F_TCA2       = 0x10;

struct __attribute__((packed)) SensorData {
  int16_t x_raw;
  int16_t y_raw;
  int16_t z_raw;
  int16_t t_raw;
  uint16_t last_sample_ms16;
  uint8_t stat2;
  uint8_t flags;
};

static SensorData g_sensors[SENSOR_COUNT];

// -----------------------------------------------------------------------------
// Indexing helpers
// -----------------------------------------------------------------------------
static uint8_t sensor_index(uint8_t tca1_ch, uint8_t tca2_ch, uint8_t addr) {
  const uint8_t base = (uint8_t)(tca1_ch * (TCA2_CH_COUNT * MLX_PER_CHANNEL) + tca2_ch * MLX_PER_CHANNEL);
  return (uint8_t)(base + ((addr == MLX_ADDR_B) ? 1 : 0));
}

static SensorData &sensor_ref(uint8_t tca1_ch, uint8_t tca2_ch, uint8_t addr) {
  return g_sensors[sensor_index(tca1_ch, tca2_ch, addr)];
}

static void decode_sensor_index(uint8_t idx, uint8_t &tca1_ch, uint8_t &tca2_ch, uint8_t &addr) {
  tca1_ch = idx / (TCA2_CH_COUNT * MLX_PER_CHANNEL);
  const uint8_t rem = idx % (TCA2_CH_COUNT * MLX_PER_CHANNEL);
  tca2_ch = rem / MLX_PER_CHANNEL;
  addr = (rem & 1) ? MLX_ADDR_B : MLX_ADDR_A;
}

static bool tca2_present_on_tca1(uint8_t tca1_ch) {
  return (g_tca2_present_mask & (uint8_t)(1u << tca1_ch)) != 0;
}

static void set_tca2_present_on_tca1(uint8_t tca1_ch, bool present) {
  if (present) g_tca2_present_mask |= (uint8_t)(1u << tca1_ch);
  else         g_tca2_present_mask &= (uint8_t)~(1u << tca1_ch);
}

static void clear_sensor_flags_runtime() {
  memset(g_sensors, 0, sizeof(g_sensors));
  g_tca2_present_mask = 0;
  g_present_sensor_count = 0;
  g_configured_sensor_count = 0;
  g_next_sensor_index = 0;
}

// -----------------------------------------------------------------------------
// Raw I2C helpers
// -----------------------------------------------------------------------------
static bool raw_i2c_ping(uint8_t addr) {
  Wire.beginTransmission(addr);
  g_last_i2c_error = Wire.endTransmission(true);
  return g_last_i2c_error == 0;
}

static bool raw_i2c_write_reg(uint8_t addr, uint8_t reg, uint8_t val) {
  Wire.beginTransmission(addr);
  Wire.write(reg);
  Wire.write(val);
  g_last_i2c_error = Wire.endTransmission(true);
  return g_last_i2c_error == 0;
}

static bool raw_i2c_write_byte(uint8_t addr, uint8_t val) {
  Wire.beginTransmission(addr);
  Wire.write(val);
  g_last_i2c_error = Wire.endTransmission(true);
  return g_last_i2c_error == 0;
}

static bool raw_i2c_read_reg(uint8_t addr, uint8_t reg, uint8_t &val) {
  Wire.beginTransmission(addr);
  Wire.write(reg);
  g_last_i2c_error = Wire.endTransmission(false);
  if (g_last_i2c_error != 0) return false;

  uint8_t got = Wire.requestFrom((int)addr, 1, (int)true);
  if (got != 1 || Wire.available() < 1) {
    g_last_i2c_error = 0xFF;
    return false;
  }

  val = (uint8_t)Wire.read();
  return true;
}

static bool raw_i2c_read_block(uint8_t addr, uint8_t reg, uint8_t *buf, uint8_t len) {
  Wire.beginTransmission(addr);
  Wire.write(reg);
  g_last_i2c_error = Wire.endTransmission(false);
  if (g_last_i2c_error != 0) return false;

  uint8_t got = Wire.requestFrom((int)addr, (int)len, (int)true);
  if (got != len) {
    g_last_i2c_error = 0xFF;
    return false;
  }

  for (uint8_t i = 0; i < len; ++i) {
    if (Wire.available() < 1) {
      g_last_i2c_error = 0xFF;
      return false;
    }
    buf[i] = (uint8_t)Wire.read();
  }
  return true;
}

static void print_i2c_error(const __FlashStringHelper *prefix) {
  Serial.print(prefix);
  if (g_last_i2c_error == 0xFF) {
    Serial.println(F("short read / no data returned"));
    return;
  }
  Serial.print(F("Wire error code "));
  Serial.println(g_last_i2c_error);
}

// -----------------------------------------------------------------------------
// TCA routing
// -----------------------------------------------------------------------------
static bool tca_select_single(uint8_t tca_addr, uint8_t channel) {
  if (channel > 7) {
    g_last_i2c_error = 4;
    return false;
  }
  return raw_i2c_write_byte(tca_addr, (uint8_t)(1u << channel));
}

static bool route_to_tca2(uint8_t tca1_channel) {
  if (!raw_i2c_ping(TCA1_ADDR)) return false;
  if (!tca_select_single(TCA1_ADDR, tca1_channel)) return false;
  if (!raw_i2c_ping(TCA2_ADDR)) return false;
  return true;
}

static bool route_to_sensor_channel(uint8_t tca1_channel, uint8_t tca2_channel) {
  if (!route_to_tca2(tca1_channel)) return false;
  return tca_select_single(TCA2_ADDR, tca2_channel);
}

// -----------------------------------------------------------------------------
// MLX I2C helpers
// -----------------------------------------------------------------------------
static bool mlx_i2c_ping(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr) {
  if (!route_to_sensor_channel(tca1_channel, tca2_channel)) return false;
  return raw_i2c_ping(addr);
}

static bool mlx_i2c_write_reg(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr, uint8_t reg, uint8_t val) {
  if (!route_to_sensor_channel(tca1_channel, tca2_channel)) return false;
  return raw_i2c_write_reg(addr, reg, val);
}

static bool mlx_i2c_read_reg(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr, uint8_t reg, uint8_t &val) {
  if (!route_to_sensor_channel(tca1_channel, tca2_channel)) return false;
  return raw_i2c_read_reg(addr, reg, val);
}

static bool mlx_i2c_read_block(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr, uint8_t reg, uint8_t *buf, uint8_t len) {
  if (!route_to_sensor_channel(tca1_channel, tca2_channel)) return false;
  return raw_i2c_read_block(addr, reg, buf, len);
}

// -----------------------------------------------------------------------------
// MLX config / acquisition
// -----------------------------------------------------------------------------
static bool mlx_power_down(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr) {
  return mlx_i2c_write_reg(tca1_channel, tca2_channel, addr, REG_CTRL1, (uint8_t)(CTRL1_XYZ_EN | MODE_POWER_DOWN));
}

static bool mlx_configure_low_noise_continuous(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr) {
  uint8_t ctrl2 = 0, ctrl3 = 0, ctrl4 = 0;

  if (!mlx_i2c_read_reg(tca1_channel, tca2_channel, addr, REG_CTRL2, ctrl2)) return false;
  if (!mlx_i2c_read_reg(tca1_channel, tca2_channel, addr, REG_CTRL3, ctrl3)) return false;
  if (!mlx_i2c_read_reg(tca1_channel, tca2_channel, addr, REG_CTRL4, ctrl4)) return false;

  ctrl2 &= (uint8_t)~0xC0;
  ctrl2 |= (uint8_t)(CONFIG_LOW_NOISE_HIGH_SENS << 6);

  ctrl3 = (uint8_t)((OSR_ENABLED << 7) | (OSR_ENABLED << 6) | (DIG_FILT_MAX << 3) | DIG_FILT_MAX);

  ctrl4 &= (uint8_t)~0x2F;
  ctrl4 |= (uint8_t)(0x80 | 0x10);
  ctrl4 |= CTRL4_T_EN;
  ctrl4 |= DIG_FILT_MAX;

  if (!mlx_i2c_write_reg(tca1_channel, tca2_channel, addr, REG_CTRL2, ctrl2)) return false;
  if (!mlx_i2c_write_reg(tca1_channel, tca2_channel, addr, REG_CTRL3, ctrl3)) return false;
  if (!mlx_i2c_write_reg(tca1_channel, tca2_channel, addr, REG_CTRL4, ctrl4)) return false;

  if (!mlx_power_down(tca1_channel, tca2_channel, addr)) return false;
  delay(2);

  if (!mlx_i2c_write_reg(tca1_channel, tca2_channel, addr, REG_CTRL1, (uint8_t)(CTRL1_XYZ_EN | MODE_CONT_5HZ))) return false;
  delay(2);
  return true;
}

static bool mlx_data_ready(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr, bool &ready) {
  uint8_t stat1 = 0;
  if (!mlx_i2c_read_reg(tca1_channel, tca2_channel, addr, REG_STAT1, stat1)) return false;
  ready = (stat1 & DRDY_BIT) != 0;
  return true;
}

static bool mlx_read_data(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr,
                          int16_t &x, int16_t &y, int16_t &z, int16_t &t, uint8_t &stat2) {
  uint8_t buf[10];
  if (!mlx_i2c_read_block(tca1_channel, tca2_channel, addr, REG_STAT1, buf, sizeof(buf))) return false;

  x = (int16_t)(((uint16_t)buf[2] << 8) | buf[1]);
  y = (int16_t)(((uint16_t)buf[4] << 8) | buf[3]);
  z = (int16_t)(((uint16_t)buf[6] << 8) | buf[5]);
  t = (int16_t)(((uint16_t)buf[9] << 8) | buf[8]);
  stat2 = buf[7];
  return true;
}

static bool detect_and_configure_sensor(uint8_t tca1_channel, uint8_t tca2_channel, uint8_t addr) {
  SensorData &s = sensor_ref(tca1_channel, tca2_channel, addr);
  s.flags = (uint8_t)((s.flags & F_TCA2) ? F_TCA2 : 0);

  if (!(s.flags & F_TCA2)) return false;
  if (!mlx_i2c_ping(tca1_channel, tca2_channel, addr)) return false;

  s.flags |= F_PRESENT;
  ++g_present_sensor_count;

  uint8_t company_id = 0;
  if (!mlx_i2c_read_reg(tca1_channel, tca2_channel, addr, REG_COMPANY_ID, company_id)) return false;

  if (!mlx_configure_low_noise_continuous(tca1_channel, tca2_channel, addr)) return false;

  s.flags |= F_CONFIGURED;
  ++g_configured_sensor_count;
  return true;
}

static bool configure_all_sensors() {
  clear_sensor_flags_runtime();

  if (!raw_i2c_ping(TCA1_ADDR)) {
    Serial.println(F("ERROR: TCA #1 (0x70) did not ACK on the root bus."));
    print_i2c_error(F("  ping failed: "));
    return false;
  }

  for (uint8_t t1 = 0; t1 < TCA1_CH_COUNT; ++t1) {
    const bool tca2_ok = route_to_tca2(t1);
    set_tca2_present_on_tca1(t1, tca2_ok);

    for (uint8_t t2 = 0; t2 < TCA2_CH_COUNT; ++t2) {
      if (tca2_ok) {
        sensor_ref(t1, t2, MLX_ADDR_A).flags |= F_TCA2;
        sensor_ref(t1, t2, MLX_ADDR_B).flags |= F_TCA2;
      }
    }

    if (!tca2_ok) continue;

    for (uint8_t t2 = 0; t2 < TCA2_CH_COUNT; ++t2) {
      detect_and_configure_sensor(t1, t2, MLX_ADDR_A);
      detect_and_configure_sensor(t1, t2, MLX_ADDR_B);
    }
  }

  Serial.print(F("Detected "));
  Serial.print(g_present_sensor_count);
  Serial.print(F(" MLX90394 device(s); configured "));
  Serial.print(g_configured_sensor_count);
  Serial.println(F(" device(s)."));
  return g_configured_sensor_count > 0;
}

static bool stop_all_sensors() {
  bool any_ok = false;
  for (uint8_t idx = 0; idx < SENSOR_COUNT; ++idx) {
    SensorData &s = g_sensors[idx];
    if ((s.flags & (F_PRESENT | F_CONFIGURED)) != (F_PRESENT | F_CONFIGURED)) continue;

    uint8_t t1, t2, addr;
    decode_sensor_index(idx, t1, t2, addr);
    if (mlx_power_down(t1, t2, addr)) {
      any_ok = true;
    } else {
      s.flags &= (uint8_t)~F_LAST_OK;
    }
  }
  g_streaming_enabled = false;
  return any_ok;
}

static void poll_one_sensor() {
  SensorData &s = g_sensors[g_next_sensor_index];

  uint8_t t1, t2, addr;
  decode_sensor_index(g_next_sensor_index, t1, t2, addr);
  g_next_sensor_index = (uint8_t)((g_next_sensor_index + 1u) % SENSOR_COUNT);

  if ((s.flags & (F_TCA2 | F_PRESENT | F_CONFIGURED)) != (F_TCA2 | F_PRESENT | F_CONFIGURED)) return;

  bool ready = false;
  if (!mlx_data_ready(t1, t2, addr, ready)) {
    s.flags &= (uint8_t)~F_LAST_OK;
    return;
  }
  if (!ready) return;

  if (!mlx_read_data(t1, t2, addr, s.x_raw, s.y_raw, s.z_raw, s.t_raw, s.stat2)) {
    s.flags &= (uint8_t)~F_LAST_OK;
    return;
  }

  s.flags |= (uint8_t)(F_HAVE | F_LAST_OK);
  s.last_sample_ms16 = (uint16_t)millis();
}

// -----------------------------------------------------------------------------
// Formatting / scan output
// -----------------------------------------------------------------------------
static const char *status_string(const SensorData &s) {
  static char buf[7];
  uint8_t pos = 0;

  if (!(s.flags & F_TCA2))      return "NOTCA2";
  if (!(s.flags & F_PRESENT))   return "ABSENT";
  if (!(s.flags & F_CONFIGURED))return "CFGERR";
  if (!(s.flags & F_HAVE))      return "NOSAMP";
  if (!(s.flags & F_LAST_OK))   return "RDERR";

  if (s.stat2 & STAT2_XOV) buf[pos++] = 'X';
  if (s.stat2 & STAT2_YOV) buf[pos++] = 'Y';
  if (s.stat2 & STAT2_ZOV) buf[pos++] = 'Z';
  if (s.stat2 & STAT2_DOR) buf[pos++] = 'D';
  if (pos == 0) buf[pos++] = '-';
  buf[pos] = '\0';
  return buf;
}

static void print_padded_float(float v, uint8_t width, uint8_t decimals) {
  char buf[20];
  dtostrf(v, width, decimals, buf);
  Serial.print(buf);
}

static void print_padded_uint(uint16_t v, uint8_t width) {
  char buf[8];
  utoa(v, buf, 10);
  const uint8_t len = (uint8_t)strlen(buf);
  for (uint8_t i = len; i < width; ++i) Serial.print(' ');
  Serial.print(buf);
}

static void print_sensor_cell(const SensorData &s, uint16_t now16) {
  if (!(s.flags & F_TCA2)) {
    Serial.print(F(" NOTCA2                                  "));
    return;
  }
  if (!(s.flags & F_PRESENT)) {
    Serial.print(F(" ABSENT                                  "));
    return;
  }
  if (!(s.flags & F_CONFIGURED)) {
    Serial.print(F(" CFGERR                                  "));
    return;
  }
  if (!(s.flags & F_HAVE)) {
    Serial.print(F(" NOSAMP                                  "));
    return;
  }

  const float bx = s.x_raw * SENS_UT_PER_LSB;
  const float by = s.y_raw * SENS_UT_PER_LSB;
  const float bz = s.z_raw * SENS_UT_PER_LSB;
  const float temp_c = s.t_raw / TEMP_LSB_PER_C;
  const uint16_t age_ms = (uint16_t)(now16 - s.last_sample_ms16);
  const char *st = status_string(s);

  Serial.print(' ');
  print_padded_float(bx, 8, 2);
  Serial.print(' ');
  print_padded_float(by, 8, 2);
  Serial.print(' ');
  print_padded_float(bz, 8, 2);
  Serial.print(' ');
  print_padded_float(temp_c, 7, 2);
  Serial.print(' ');
  print_padded_uint(age_ms, 6);
  Serial.print(' ');
  Serial.print(st);
  const uint8_t slen = (uint8_t)strlen(st);
  for (uint8_t i = slen; i < 6; ++i) Serial.print(' ');
}

static void root_i2c_scan() {
  Serial.println(F("Root-bus I2C scan:"));
  bool found_any = false;
  for (uint8_t addr = 0x08; addr < 0x78; ++addr) {
    if (raw_i2c_ping(addr)) {
      Serial.print(F("  found 0x"));
      if (addr < 0x10) Serial.print('0');
      Serial.println(addr, HEX);
      found_any = true;
    }
  }
  if (!found_any) Serial.println(F("  no devices responded"));
}

static void downstream_scan() {
  Serial.println(F("Second-stage TCA / MLX scan behind root TCA 0x70:"));
  for (uint8_t t1 = 0; t1 < TCA1_CH_COUNT; ++t1) {
    Serial.print(F("  TCA1 ch"));
    Serial.print(t1);
    Serial.print(F(": "));

    if (!route_to_tca2(t1)) {
      Serial.println(F("no TCA2 at 0x77"));
      continue;
    }

    Serial.println(F("TCA2 0x77 present"));
    for (uint8_t t2 = 0; t2 < TCA2_CH_COUNT; ++t2) {
      Serial.print(F("    TCA2 ch"));
      Serial.print(t2);
      Serial.print(F(": "));
      bool any = false;

      if (mlx_i2c_ping(t1, t2, MLX_ADDR_A)) {
        Serial.print(F("0x60 "));
        any = true;
      }
      if (mlx_i2c_ping(t1, t2, MLX_ADDR_B)) {
        Serial.print(F("0x61 "));
        any = true;
      }
      if (!any) Serial.print(F("none"));
      Serial.println();
    }
  }
}

static void i2c_scan() {
  root_i2c_scan();
  downstream_scan();
}

static bool branch_has_present_sensors(uint8_t tca1_channel) {
  for (uint8_t t2 = 0; t2 < TCA2_CH_COUNT; ++t2) {
    const SensorData &a = sensor_ref(tca1_channel, t2, MLX_ADDR_A);
    const SensorData &b = sensor_ref(tca1_channel, t2, MLX_ADDR_B);
    if ((a.flags & F_PRESENT) || (b.flags & F_PRESENT)) return true;
  }
  return false;
}

static void print_snapshot_block(uint8_t tca1_channel, uint16_t now16) {
  Serial.println(F("=============================================================================================================="));
  Serial.print(F("TCA1 ch"));
  Serial.print(tca1_channel);
  if (tca2_present_on_tca1(tca1_channel)) Serial.println(F("  ->  TCA2 0x77"));
  else                                     Serial.println(F("  ->  no TCA2 0x77"));

  Serial.println(F("CH | Addr  |        X        Y        Z       T    Age Flags | Addr  |        X        Y        Z       T    Age Flags"));
  Serial.println(F("--------------------------------------------------------------------------------------------------------------"));

  for (uint8_t t2 = 0; t2 < TCA2_CH_COUNT; ++t2) {
    const SensorData &a = sensor_ref(tca1_channel, t2, MLX_ADDR_A);
    const SensorData &b = sensor_ref(tca1_channel, t2, MLX_ADDR_B);

    Serial.print(' ');
    Serial.print(t2);
    Serial.print(F(" | 0x60 |"));
    print_sensor_cell(a, now16);
    Serial.print(F(" | 0x61 |"));
    print_sensor_cell(b, now16);
    Serial.println();
  }
}

static void print_snapshot() {
  const uint16_t now16 = (uint16_t)millis();

  Serial.println();
  Serial.println(F("##############################################################################################################"));
  Serial.print(F("Snapshot @ "));
  Serial.print((unsigned long)millis());
  Serial.println(F(" ms   (units: uT, uT, uT, C, age_ms, flags)"));
  Serial.print(F("Detected TCA2 branches with populated sensors: "));

  bool any_printed_branch = false;
  for (uint8_t t1 = 0; t1 < TCA1_CH_COUNT; ++t1) {
    if (!tca2_present_on_tca1(t1)) continue;
    if (!branch_has_present_sensors(t1)) continue;
    if (any_printed_branch) Serial.print(F(", "));
    Serial.print(F("ch"));
    Serial.print(t1);
    any_printed_branch = true;
  }
  if (!any_printed_branch) Serial.print(F("none"));
  Serial.println();

  Serial.print(F("Present MLX90394 devices: "));
  Serial.print(g_present_sensor_count);
  Serial.print(F("   Configured: "));
  Serial.println(g_configured_sensor_count);

  for (uint8_t t1 = 0; t1 < TCA1_CH_COUNT; ++t1) {
    if (!branch_has_present_sensors(t1)) continue;
    print_snapshot_block(t1, now16);
  }
  Serial.println(F("##############################################################################################################"));
}

// -----------------------------------------------------------------------------
// Serial command handling without Arduino String
// -----------------------------------------------------------------------------
static bool read_serial_line(char *buf, uint8_t buf_size) {
  static uint8_t pos = 0;
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') {
      buf[pos] = '\0';
      pos = 0;
      return true;
    }
    if (pos < (uint8_t)(buf_size - 1)) {
      buf[pos++] = c;
    }
  }
  return false;
}

static void handle_serial_command(const char *cmd) {
  if (strcmp(cmd, "scan") == 0) {
    i2c_scan();
    return;
  }
  if (strcmp(cmd, "stop") == 0) {
    if (!stop_all_sensors()) {
      Serial.println(F("ERROR: Failed to stop one or more sensors."));
      print_i2c_error(F("  stop failed: "));
      return;
    }
    Serial.println(F("All configured sensors powered down."));
    return;
  }
  if (strcmp(cmd, "start") == 0) {
    if (!configure_all_sensors()) {
      Serial.println(F("ERROR: Failed to configure/start sensors."));
      print_i2c_error(F("  start failed: "));
      return;
    }
    g_streaming_enabled = true;
    Serial.println(F("All detected sensors restarted in 5 Hz continuous mode."));
    return;
  }
}

// -----------------------------------------------------------------------------
// Arduino sketch
// -----------------------------------------------------------------------------
void setup() {
  Serial.begin(250000);
  while (!Serial) {
    delay(10);
  }

  Wire.begin();
  Wire.setClock(100000);
  delay(20);

  if (!configure_all_sensors()) {
    i2c_scan();
    return;
  }

  g_streaming_enabled = true;
  g_last_snapshot_ms = millis();

  Serial.println(F("Streaming MLX90394 tree via 0x70 ch[0..6] -> 0x77 ch[0..7]."));
  Serial.println(F("Low-RAM build: cached samples only, no per-sensor counters/IDs."));
  Serial.println(F("Per-sensor settings: CONFIG=2, OSR_HALL=1, OSR_TEMP=1, DIG_FILT_XY=7, DIG_FILT_Z=7, DIG_FILT_TEMP=7, mode=5Hz."));
  Serial.println(F("Commands: scan | stop | start"));
}

void loop() {
  char cmd[12];
  if (read_serial_line(cmd, sizeof(cmd))) {
    if (cmd[0] != '\0') handle_serial_command(cmd);
  }

  if (!g_streaming_enabled) {
    delay(5);
    return;
  }

  poll_one_sensor();

  const uint32_t now = millis();
  if ((uint32_t)(now - g_last_snapshot_ms) >= SNAPSHOT_PERIOD_MS) {
    g_last_snapshot_ms = now;
    print_snapshot();
  }
}
