import "server-only";

import { PrismaClient } from "@prisma/client";

const globalForPrisma = globalThis as unknown as {
  prisma?: PrismaClient;
};

function getDatasourceUrl(): string | undefined {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    return undefined;
  }

  try {
    const url = new URL(databaseUrl);

    if (!url.searchParams.has("connection_limit")) {
      url.searchParams.set("connection_limit", "1");
    }

    return url.toString();
  } catch {
    if (databaseUrl.includes("connection_limit=")) {
      return databaseUrl;
    }

    const separator = databaseUrl.includes("?") ? "&" : "?";
    return `${databaseUrl}${separator}connection_limit=1`;
  }
}

export const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    datasourceUrl: getDatasourceUrl(),
    log: process.env.NODE_ENV === "development" ? ["warn", "error"] : ["error"],
  });

if (process.env.NODE_ENV !== "production") {
  globalForPrisma.prisma = prisma;
}