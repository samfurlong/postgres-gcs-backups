import { Pool } from 'pg';
import fs from 'fs/promises';
import zlib from 'zlib';
import { promisify } from 'util';
import { env } from "./env";

const gzip = promisify(zlib.gzip);

const backupToFile = async (path: string) => {
  console.log("Starting DB dump to file...");

  const pool = new Pool({
    connectionString: env.BACKUP_DATABASE_URL,
  });

  let client;
  try {
    client = await pool.connect();
    console.log("Connected to database");

    let backupContent = '';

    // Get all tables
    const tableQuery = await client.query(`
      SELECT schemaname, tablename 
      FROM pg_catalog.pg_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
      ORDER BY schemaname, tablename
    `);

    console.log(`Found ${tableQuery.rows.length} tables`);

    for (const row of tableQuery.rows) {
      const schemaName = row.schemaname;
      const tableName = row.tablename;
      const fullTableName = `"${schemaName}"."${tableName}"`;
      console.log(`Processing table: ${fullTableName}`);

      // Get table schema
      const schemaQuery = await client.query(`
        SELECT pg_dump_table_schema('${fullTableName}') as schema
      `);
      backupContent += schemaQuery.rows[0].schema + '\n';

      // Get table data
      const dataQuery = await client.query(`SELECT * FROM ${fullTableName}`);
      console.log(`Table ${fullTableName} has ${dataQuery.rows.length} rows`);

      for (const dataRow of dataQuery.rows) {
        backupContent += JSON.stringify(dataRow) + '\n';
      }

      // If backupContent is getting large, write to file and reset
      if (backupContent.length > 1000000) { // 1MB
        await fs.appendFile(path, await gzip(backupContent));
        backupContent = '';
        console.log(`Wrote chunk to ${path}`);
      }
    }

    // Write any remaining content
    if (backupContent.length > 0) {
      await fs.appendFile(path, await gzip(backupContent));
      console.log(`Wrote final chunk to ${path}`);
    }

  } catch (error) {
    console.error("Error during backup:", error);
    throw error;
  } finally {
    if (client) client.release();
    await pool.end();
  }

  const stats = await fs.stat(path);
  console.log(`Backup file size: ${stats.size} bytes`);
  console.log("DB dump to file completed");
};
