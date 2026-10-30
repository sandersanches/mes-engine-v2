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
  value: number; // Contagem bruta do canal
}

interface InfluxRowRecord {
  _value: number | string | boolean;
  _time: string;
  [key: string]: unknown;
}

export async function fetchPointsAfterDirect(params: {
  bucket: string;
  deviceId: string;
  channel: string;
  afterTimestampSeconds: number;
  limit: number;
}): Promise<TelemetryFeedPoint[]> {
  const { bucket, deviceId, channel, afterTimestampSeconds, limit } = params;

  // Janela inicial de busca baseada no último timestamp processado
  const startIso =
    afterTimestampSeconds > 0
      ? new Date(afterTimestampSeconds * 1000).toISOString()
      : new Date(Date.now() - 24 * 3600 * 1000).toISOString();

  // Consulta Flux direta e ordenada pelo tempo
  const fluxQuery = `
    from(bucket: "${bucket}")
      |> range(start: ${startIso})
      |> filter(fn: (r) => r._measurement == "device_telemetry" and r._field == "value" and r.channel == "${channel}" and r.deviceId == "${deviceId}")
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
