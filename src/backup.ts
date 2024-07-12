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
    const tableQuery = await client.query(`
      SELECT tablename FROM pg_tables 
      WHERE schemaname = 'public'
    `);

    const gzip = zlib.createGzip();
    const writeStream = fs.createWriteStream(path);

    await pipeline(gzip, writeStream);

    for (const row of tableQuery.rows) {
      const tableName = row.tablename;
      console.log(`Backing up table: ${tableName}`);

      // Write table schema
      const schemaQuery = await client.query(`
        SELECT pg_dump_table_schema('${tableName}') as schema
      `);
      gzip.write(schemaQuery.rows[0].schema + '\n');

      // Stream table data
      const dataQuery = client.query(`COPY ${tableName} TO STDOUT`);
      dataQuery.on('row', (row) => {
        gzip.write(row + '\n');
      });

      await new Promise((resolve) => dataQuery.on('end', resolve));
    }

    gzip.end();
    client.release();
  } finally {
    await pool.end();
  }

  console.log("DB dumped to file...");
};

const uploadToGCS = async ({ name, path }: { name: string; path: string }) => {
  console.log("Uploading backup to GCS...");

  const bucketName = env.GCS_BUCKET;

  const uploadOptions: UploadOptions = {
    destination: name,
  };

  const storage = new Storage({
    projectId: env.GOOGLE_PROJECT_ID,
    credentials: JSON.parse(env.SERVICE_ACCOUNT_JSON),
  });

  await storage.bucket(bucketName).upload(path, uploadOptions);

  console.log("Backup uploaded to GCS...");
};

const deleteFile = async (path: string) => {
  console.log("Deleting file...");
  await fs.promises.unlink(path);
};

export const backup = async () => {
  console.log("Initiating DB backup...");

  let date = new Date().toISOString();
  const timestamp = date.replace(/[:.]+/g, "-");
  const filename = `${env.BACKUP_PREFIX}backup-${timestamp}.gz`;
  const filepath = `/tmp/${filename}`;

  try {
    await backupToFile(filepath);
    
    const { size } = await fs.promises.stat(filepath);
    console.log(`Backup file size: ${size} bytes`);
    
    if (size < 1000) {
      throw new Error(`Backup file is too small (${size} bytes). Possible dump failure.`);
    }
    
    await uploadToGCS({ name: filename, path: filepath });
  } catch (error) {
    console.error("Backup failed:", error);
    throw error; // Re-throw the error for the caller to handle
  } finally {
    await deleteFile(filepath).catch(console.error);
  }

  console.log("DB backup complete...");
};
