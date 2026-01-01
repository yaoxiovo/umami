export const PRISMA = 'prisma';
export const POSTGRESQL = 'postgresql';

// Fixes issue with converting bigint values
BigInt.prototype.toJSON = function () {
  return Number(this);
};

export function getDatabaseType(url = process.env.DATABASE_URL) {
  const type = url?.split(':')[0];

  if (type === 'postgres') {
    return POSTGRESQL;
  }

  return type;
}

export async function runQuery(queries: any) {
  return queries[PRISMA]();
}

export function notImplemented() {
  throw new Error('Not implemented.');
}
