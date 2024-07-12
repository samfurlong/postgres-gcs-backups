import { exec } from "child_process";
import fs from "fs/promises";
import { Storage, UploadOptions } from "@google-cloud/storage";
import { env } from "./env";

const logToFile = async (message: string) => {
  const logPath = '/tmp/backup_log.txt';
  await fs.appendFile(logPath, `${new Date().toISOString()}: ${message}\n`);
};

const dumpToFile = async (path: string) => {
  await logToFile("Starting database dump...");

  return new Promise((resolve, reject) => {
    // Using process.env for Postgres-specific variables
    const command = `pg_dump -h ${process.env.PGHOST} -p ${process.env.PGPORT} -U ${process.env.PGUSER} -d ${process.env.PGDATABASE} -f ${path}`;
    
    exec(command, async (error, stdout, stderr) => {
      if (error) {
        await logToFile(`Dump error: ${error.message}`);
        reject(error);
        return;
      }
      if (stderr) {
        await logToFile(`Dump stderr: ${stderr}`);
      }
      if (stdout) {
        await logToFile(`Dump stdout: ${stdout}`);
      }
      
      const stats = await fs.stat(path);
      await logToFile(`Dump completed. File size: ${stats.size} bytes`);
      resolve(undefined);
    });
  });
};

// ... (other functions remain the same)

export const backup = async () => {
  await logToFile("Initiating DB backup process...");

  const timestamp = new Date().toISOString().replace(/[:.]+/g, "-");
  const filename = `backup-${timestamp}.sql`;
  const filepath = `/tmp/${filename}`;

  try {
    // PGPASSWORD is already set in the environment, no need to set it here

    await dumpToFile(filepath);
    
    const stats = await fs.stat(filepath);
    await logToFile(`Backup file created. Size: ${stats.size} bytes`);
    
    if (stats.size < 1000) {
      throw new Error(`Backup file is too small (${stats.size} bytes). Possible dump failure.`);
    }
    
    await uploadToGCS({ name: filename, path: filepath });
  } catch (error) {
    await logToFile(`Backup failed: ${error}`);
    throw error;
  } finally {
    await deleteFile(filepath);
  }

  await logToFile("DB backup process completed.");
};
