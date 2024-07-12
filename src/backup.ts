import { Pool } from 'pg';
import fs from 'fs';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';

const backupToFile = async (path: string) => {
  console.log("Initiating DB dump to file...");

  const pool = new Pool({
    connectionString: env.BACKUP_DATABASE_URL,
  });

  try {
    // Test the connection
    const client = await pool.connect();
    console.log("Successfully connected to the database.");

    // Check user permissions
    const userCheck = await client.query(`
      SELECT current_user, current_database(), 
             has_database_privilege(current_user, current_database(), 'CONNECT') as can_connect,
             has_schema_privilege(current_user, 'public', 'USAGE') as can_use_public,
             has_schema_privilege(current_user, 'information_schema', 'USAGE') as can_use_info_schema
    `);
    console.log("User permissions:", userCheck.rows[0]);

    // Check accessible tables
    const tableCheck = await client.query(`
      SELECT schemaname, tablename 
      FROM pg_catalog.pg_tables
      WHERE has_table_privilege(current_user, schemaname || '.' || tablename, 'SELECT')
    `);
    console.log("Accessible tables:", tableCheck.rows);

    if (tableCheck.rows.length === 0) {
      throw new Error("No accessible tables found. Check database permissions.");
    }

    const gzip = zlib.createGzip();
    const writeStream = fs.createWriteStream(path);

    await pipeline(gzip, writeStream);

    for (const row of tableCheck.rows) {
      const schemaName = row.schemaname;
      const tableName = row.tablename;
      const fullTableName = `"${schemaName}"."${tableName}"`;
      console.log(`Attempting to backup table: ${fullTableName}`);

      try {
        // Write table schema
        const schemaQuery = await client.query(`
          SELECT pg_dump_table_schema('${fullTableName}') as schema
        `);
        gzip.write(schemaQuery.rows[0].schema + '\n');

        // Stream table data
        const dataQuery = client.query(`COPY ${fullTableName} TO STDOUT`);
        let rowCount = 0;
        dataQuery.on('row', (row) => {
          gzip.write(row + '\n');
          rowCount++;
        });

        await new Promise((resolve) => dataQuery.on('end', resolve));
        console.log(`Backed up ${rowCount} rows from ${fullTableName}`);
      } catch (error) {
        console.error(`Error backing up table ${fullTableName}:`, error);
      }
    }

    gzip.end();
    client.release();
  } catch (error) {
    console.error("Error during backup process:", error);
    throw error;
  } finally {
    await pool.end();
  }

  const stats = await fs.promises.stat(path);
  console.log(`DB dump completed. File size: ${stats.size} bytes`);
};
