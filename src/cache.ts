import { controlPlaneDb, getTenantClient } from './db.js';
import { logger } from './logger.js';

export interface CachedShift {
  id: string;
  name: string;
  startTime: string; // "06:00"
  endTime: string; // "14:20"
  crossesMidnight: boolean;
}

export interface CachedTenant {
  id: string;
  name: string;
  subdomain: string;
  influxBucket: string;
  stopToleranceSeconds: number;
}

interface TenantShiftsCache {
  lastUpdated: number;
  shifts: CachedShift[];
}

const CONFIG_CACHE_TTL_MS =
  (Number(process.env.CONFIG_CACHE_TTL_MINUTES) || 3) * 60 * 1000;
const SHIFTS_CACHE_TTL_MS =
  (Number(process.env.SHIFTS_CACHE_TTL_MINUTES) || 3) * 60 * 1000;

let tenantsCache: { lastUpdated: number; tenants: CachedTenant[] } = {
  lastUpdated: 0,
  tenants: [],
};

const shiftsCacheByTenant = new Map<string, TenantShiftsCache>();

// Retorna os tenants ativos (atualiza a cada 3 min)
export async function getActiveTenants(): Promise<CachedTenant[]> {
  const now = Date.now();
  if (
    now - tenantsCache.lastUpdated < CONFIG_CACHE_TTL_MS &&
    tenantsCache.tenants.length > 0
  ) {
    return tenantsCache.tenants;
  }

  try {
    const rawTenants = await (controlPlaneDb as any).tenant.findMany({
      where: { deletedAt: null, status: 'ACTIVE' },
      select: {
        id: true,
        name: true,
        subdomain: true,
        influxBucket: true,
        stopToleranceSeconds: true,
      },
    });

    const mappedTenants: CachedTenant[] = rawTenants.map((t: any) => ({
      id: t.id,
      name: t.name,
      subdomain: t.subdomain,
      influxBucket:
        t.influxBucket || `mes_bucket_${t.subdomain.replace(/-/g, '_')}`,
      stopToleranceSeconds: t.stopToleranceSeconds || 30,
    }));

    tenantsCache = {
      lastUpdated: now,
      tenants: mappedTenants,
    };

    logger.debug(
      `[Cache] Configuração de ${mappedTenants.length} tenant(s) renovada no cache.`,
    );
    return mappedTenants;
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error('[Cache] Falha ao consultar tenants no Control Plane:', msg);
    return tenantsCache.tenants;
  }
}

// Retorna os turnos de uma empresa (atualiza a cada 3 min)
export async function getTenantShifts(
  subdomain: string,
): Promise<CachedShift[]> {
  const now = Date.now();
  const cached = shiftsCacheByTenant.get(subdomain);

  if (cached && now - cached.lastUpdated < SHIFTS_CACHE_TTL_MS) {
    return cached.shifts;
  }

  try {
    const tenantDb = await getTenantClient<any>(subdomain);
    const rawShifts = await tenantDb.shift.findMany({
      where: { deletedAt: null },
      orderBy: { startTime: 'asc' },
    });

    const shifts: CachedShift[] = rawShifts.map((s: any) => ({
      id: s.id,
      name: s.name,
      startTime: s.startTime,
      endTime: s.endTime,
      crossesMidnight: Boolean(s.crossesMidnight),
    }));

    shiftsCacheByTenant.set(subdomain, {
      lastUpdated: now,
      shifts,
    });

    logger.debug(
      `[Cache] Turnos de "${subdomain}" renovados (${shifts.length} turno(s)).`,
    );
    return shifts;
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(
      `[Cache] Falha ao consultar turnos do tenant ${subdomain}:`,
      msg,
    );
    return cached?.shifts || [];
  }
}

// Identifica em qual turno determinado horário (HH:mm) se enquadra (ou null se fora de turno)
export function resolveShiftForTimestamp(
  shifts: CachedShift[],
  timestampMs: number,
): CachedShift | null {
  if (shifts.length === 0) return null;

  const date = new Date(timestampMs);
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const currentTime = `${hours}:${minutes}`;

  for (const shift of shifts) {
    if (shift.crossesMidnight) {
      // Ex: 22:00 até 05:00
      if (currentTime >= shift.startTime || currentTime < shift.endTime) {
        return shift;
      }
    } else {
      // Ex: 06:00 até 14:20
      if (currentTime >= shift.startTime && currentTime < shift.endTime) {
        return shift;
      }
    }
  }

  return null;
}
