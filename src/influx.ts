// mes-engine\src\influx.ts
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
  channel: string; // Ex: "chd1_value", "ch1_user_range"
  value: number; // Valor lido
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
  channels: string[];
  afterTimestampSeconds: number;
  limit: number;
}): Promise<TelemetryFeedPoint[]> {
  const { bucket, deviceId, channels, afterTimestampSeconds, limit } = params;

  if (channels.length === 0) return [];

  const startIso =
    afterTimestampSeconds > 0
      ? new Date(afterTimestampSeconds * 1000).toISOString()
      : new Date(Date.now() - 24 * 3600 * 1000).toISOString();

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

/**
 * Busca pontos cronológicos (mais antigos -> mais recentes) para variáveis de processo
 * do Nexus Guardian a partir do timestamp exato (em ms) da última leitura processada.
 */
export async function fetchProcessVariablePointsAfter(params: {
  bucket: string;
  deviceId: string;
  channel: string;
  measurement?: string;
  field?: string;
  afterTimestampMs: number;
  limit: number;
}): Promise<TelemetryFeedPoint[]> {
  const {
    bucket,
    deviceId,
    channel,
    measurement = 'device_telemetry',
    field = 'value',
    afterTimestampMs,
    limit,
  } = params;

  const startIso =
    afterTimestampMs > 0
      ? new Date(afterTimestampMs).toISOString()
      : new Date(Date.now() - 24 * 3600 * 1000).toISOString();

  const fluxQuery = `
    from(bucket: "${bucket}")
      |> range(start: ${startIso})
      |> filter(fn: (r) => r._measurement == "${measurement}" and r._field == "${field}" and r.deviceId == "${deviceId}" and r.channel == "${channel}")
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

          let numericVal: number | null = null;
          if (typeof o._value === 'number' && !Number.isNaN(o._value)) {
            numericVal = o._value;
          } else if (typeof o._value === 'boolean') {
            numericVal = o._value ? 1 : 0;
          }

          if (numericVal !== null) {
            points.push({
              timestampSeconds: Math.floor(timeMs / 1000),
              timestampMs: timeMs,
              channel: String(o.channel),
              value: numericVal,
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
      `[InfluxGuardian] Erro na query Flux para "${deviceId}" (${channel}):`,
      msg,
    );
    return [];
  }
}
