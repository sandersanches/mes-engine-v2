// import { logger } from './logger.js';

// export interface TelemetryFeedPoint {
//   timestampSeconds: number; // 10 dígitos Unix
//   timestampMs: number; // 13 dígitos
//   value: number; // Contagem bruta do canal
// }

// interface TelemetryPointsResponse {
//   deviceId: string;
//   field: string;
//   count: number;
//   points: TelemetryFeedPoint[];
// }

// const TELEMETRY_API_URL =
//   process.env.TELEMETRY_API_URL || 'http://localhost:3002';
// const INTERNAL_SECRET =
//   process.env.INTERNAL_SECRET || 'nexus_sim_sec_2026_98a72b8c9d0e1f';

// export async function fetchPointsAfter(params: {
//   bucket: string;
//   deviceId: string;
//   channel: string;
//   afterTimestampSeconds: number;
//   limit: number;
// }): Promise<TelemetryFeedPoint[]> {
//   const { bucket, deviceId, channel, afterTimestampSeconds, limit } = params;

//   const queryParams = new URLSearchParams({
//     bucket,
//     deviceId,
//     field: channel,
//     afterTimestamp: String(afterTimestampSeconds),
//     limit: String(limit),
//   });

//   const url = `${TELEMETRY_API_URL}/api/telemetry/points-after?${queryParams.toString()}`;

//   try {
//     const res = await fetch(url, {
//       method: 'GET',
//       headers: {
//         'x-internal-secret': INTERNAL_SECRET,
//       },
//     });

//     if (!res.ok) {
//       const errBody = await res.json().catch(() => ({}));
//       logger.warn(
//         `[TelemetryClient] Erro HTTP ${res.status} ao consultar ${deviceId}:`,
//         (errBody as { error?: string }).error || res.statusText,
//       );
//       return [];
//     }

//     const data = (await res.json()) as TelemetryPointsResponse;
//     return data.points || [];
//   } catch (err: unknown) {
//     const msg = err instanceof Error ? err.message : String(err);
//     logger.error(
//       `[TelemetryClient] Falha de conexão com a Telemetry API (${url}):`,
//       msg,
//     );
//     return [];
//   }
// }
