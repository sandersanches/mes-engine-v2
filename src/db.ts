import path from 'path';
import { pathToFileURL } from 'url';

// 1. Resolve os caminhos absolutos dos clients gerados no backend
const controlPlaneGeneratedPath = path.resolve(
  process.cwd(),
  '../backend/src/generated/control-plane/index.js',
);

const tenantGeneratedPath = path.resolve(
  process.cwd(),
  '../backend/src/generated/tenant/index.js',
);

// 2. Converte os caminhos para URLs compatíveis com ESM no Windows (file:///C:/...)
const controlPlaneUrl = pathToFileURL(controlPlaneGeneratedPath).href;
const tenantUrl = pathToFileURL(tenantGeneratedPath).href;

// 3. Instância do Control Plane (Banco Central)
interface ControlPlaneModule {
  PrismaClient: new (options?: {
    datasources?: { db?: { url?: string } };
  }) => unknown;
}

const cpModule = (await import(controlPlaneUrl)) as ControlPlaneModule;

export const controlPlaneDb = new cpModule.PrismaClient({
  datasources: {
    db: {
      url: process.env.DATABASE_URL,
    },
  },
}) as any;

// 4. Instâncias Dinâmicas dos Bancos dos Tenants
interface TenantPrismaModule {
  PrismaClient: new (options: {
    datasources: { db: { url: string } };
  }) => unknown;
}

const tenantClients = new Map<string, unknown>();

export async function getTenantClient<T>(subdomain: string): Promise<T> {
  const normalized = subdomain.toLowerCase().replace(/[^a-z0-9]/g, '_');

  if (tenantClients.has(normalized)) {
    return tenantClients.get(normalized) as T;
  }

  const dbHost = process.env.DB_HOST || 'localhost';
  const dbPort = Number(process.env.DB_PORT) || 5432;
  const dbUser = process.env.DB_USER || 'nexus_admin';
  const dbPass = process.env.DB_PASS || 'nexus_secure_password_2026';
  const dbName = `mes_tenant_${normalized}`;

  const databaseUrl = `postgresql://${dbUser}:${dbPass}@${dbHost}:${dbPort}/${dbName}?schema=public`;

  const tenantModule = (await import(tenantUrl)) as TenantPrismaModule;
  const clientInstance = new tenantModule.PrismaClient({
    datasources: {
      db: { url: databaseUrl },
    },
  });

  tenantClients.set(normalized, clientInstance);
  return clientInstance as T;
}
