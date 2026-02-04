from common import get_session, obter_token, get_db_conn, BASE_URL

def sync_metadata():
    session = get_session()
    token = obter_token(session)
    headers = {"Authorization": f"Bearer {token}"}

    conn = get_db_conn()
    cur = conn.cursor()

    r = session.get(f"{BASE_URL}/UserDevices", headers=headers)
    r.raise_for_status()

    for device in r.json():
        cur.execute("""
            INSERT INTO devices (
                device_id, device_name, serial_number, status,
                latitude, longitude, last_upload, battery_percent
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (device_id) DO UPDATE SET
                device_name = EXCLUDED.device_name,
                status = EXCLUDED.status,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                last_upload = EXCLUDED.last_upload,
                battery_percent = EXCLUDED.battery_percent;
        """, (
            device["deviceId"],
            device["deviceName"],
            device.get("serialNumber"),
            device.get("status"),
            device.get("latitude"),
            device.get("longitude"),
            device.get("lastUpload"),
            device.get("batteryPercentage")
        ))

        for sensor in device.get("sensors", []):
            cur.execute("""
                INSERT INTO sensores (
                    sensor_id, device_id, nome_customizado,
                    tipo_sensor, unidade_medida
                )
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (sensor_id) DO UPDATE SET
                    device_id = EXCLUDED.device_id,
                    nome_customizado = EXCLUDED.nome_customizado,
                    tipo_sensor = EXCLUDED.tipo_sensor,
                    unidade_medida = EXCLUDED.unidade_medida;
            """, (
                sensor["sensorId"],
                device["deviceId"],
                sensor.get("customName") or f"Sensor {sensor['sensorId']}",
                sensor.get("sensorType"),
                sensor.get("uom")
            ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Metadata sincronizada")

if __name__ == "__main__":
    sync_metadata()
