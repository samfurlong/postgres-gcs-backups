import { Pool } from 'pg';
import fs from 'fs';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';
import { Storage, UploadOptions } from "@google-cloud/storage";
import { env } from "./env";

const backupToFile = async (path: string) => {
  console.log("Dumping DB to file...");

  const pool = new Pool({
    connectionString: env.BACKUP_DATABASE_URL,
  });

  try {
    const client = await pool.connect();
    
    // Query to get all tables from all schemas, excluding system schemas
    const tableQuery = await client.query(`
      SELECT schemaname, tablename 
      FROM pg_catalog.pg_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
      ORDER BY schemaname, tablename
    `);

    const gzip = zlib.createGzip();
    const writeStream = fs.createWriteStream(path);

    await pipeline(gzip, writeStream);

    for (const row of tableQuery.rows) {
      const schemaName = row.schemaname;
      const tableName = row.tablename;
      const fullTableName = `"${schemaName}"."${tableName}"`;
      console.log(`Backing up table: ${fullTableName}`);

      // Write table schema
      const schemaQuery = await client.query(`
        SELECT pg_dump_table_schema('${fullTableName}') as schema
      `);
      gzip.write(schemaQuery.rows[0].schema + '\n');

      // Stream table data
      const dataQuery = client.query(`COPY ${fullTableName} TO STDOUT`);
      dataQuery.on('row', (row) => {
        gzip.write(row + '\n');
      });

      await new Promise((resolve) => dataQuery.on('end', resolve));
    }

    // Backup sequences
    const sequenceQuery = await client.query(`
      SELECT sequence_schema, sequence_name
      FROM information_schema.sequences
      WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY sequence_schema, sequence_name
    `);

    for (const row of sequenceQuery.rows) {
      const schemaName = row.sequence_schema;
      const sequenceName = row.sequence_name;
      const fullSequenceName = `"${schemaName}"."${sequenceName}"`;
      console.log(`Backing up sequence: ${fullSequenceName}`);

      const sequenceDataQuery = await client.query(`
        SELECT last_value, start_value, increment_by, max_value, min_value, cache_value, log_cnt, is_cycled, is_called
        FROM ${fullSequenceName}
      `);
      
      const sequenceData = sequenceDataQuery.rows[0];
      const sequenceCreateSQL = `
        CREATE SEQUENCE IF NOT EXISTS ${fullSequenceName}
        START WITH ${sequenceData.last_value}
        INCREMENT BY ${sequenceData.increment_by}
        MINVALUE ${sequenceData.min_value}
        MAXVALUE ${sequenceData.max_value}
        CACHE ${sequenceData.cache_value}
        ${sequenceData.is_cycled ? 'CYCLE' : 'NO CYCLE'};
      `;

      gzip.write(sequenceCreateSQL + '\n');
    }

    gzip.end();
    client.release();
  } finally {
    await pool.end();
  }

  console.log("DB dumped to file...");
};

// ... (rest of the code remains the same)
