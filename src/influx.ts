import { InfluxDB } from '@influxdata/influxdb-client';
import { logger } from './logger.js';

const url = process.env.INFLUX_URL || 'http://localhost:8086';
const token =
  process.env.INFLUX_TOKEN || 'nexus_influx_super_token_secret_2026';
const org = process.env.INFLUX_ORG || 'nexus_mes';

const influx = new InfluxDB({ url, token });
const queryApi = influx.getQueryApi(org);

export interface TelemetryFeedPoint {
  timestampSeconds: number; // 10 dígitos Unix
  timestampMs: number; // 13 dígitos
  channel: string; // Ex: "chd1_value", "chd2_value"
  value: number; // Contagem bruta
}

interface InfluxRowRecord {
  _value: number | string | boolean;
  _time: string;
  channel: string;
  [key: string]: unknown;
}

export async function fetchPointsAfterDirect(params: {
  bucket: string;
  deviceId: string;
  channels: string[]; // Busca produção e refugo juntos
  afterTimestampSeconds: number;
  limit: number;
}): Promise<TelemetryFeedPoint[]> {
  const { bucket, deviceId, channels, afterTimestampSeconds, limit } = params;

  if (channels.length === 0) return [];

  const startIso =
    afterTimestampSeconds > 0
      ? new Date(afterTimestampSeconds * 1000).toISOString()
      : new Date(Date.now() - 24 * 3600 * 1000).toISOString();

  // Constrói filtro OR para os canais (ex: r.channel == "chd1_value" or r.channel == "chd2_value")
  const channelFilter = channels.map((c) => `r.channel == "${c}"`).join(' or ');

  const fluxQuery = `
    from(bucket: "${bucket}")
      |> range(start: ${startIso})
      |> filter(fn: (r) => r._measurement == "device_telemetry" and r._field == "value" and r.deviceId == "${deviceId}")
      |> filter(fn: (r) => ${channelFilter})
      |> filter(fn: (r) => uint(v: r._time) > uint(v: time(v: "${startIso}")))
      |> sort(columns: ["_time"], desc: false)
      |> limit(n: ${limit})
  `;

  try {
    const points: TelemetryFeedPoint[] = [];

    await new Promise<void>((resolve, reject) => {
      queryApi.queryRows(fluxQuery, {
        next(row, tableMeta) {
          const o = tableMeta.toObject(row) as InfluxRowRecord;
          const timeMs = new Date(o._time).getTime();
          if (typeof o._value === 'number') {
            points.push({
              timestampSeconds: Math.floor(timeMs / 1000),
              timestampMs: timeMs,
              channel: String(o.channel),
              value: o._value,
            });
          }
        },
        error(err) {
          reject(err);
        },
        complete() {
          resolve();
        },
      });
    });

    return points;
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(
      `[InfluxDirect] Erro na query Flux para dispositivo "${deviceId}":`,
      msg,
    );
    return [];
  }
}
