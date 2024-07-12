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
    // Using the provided environment variables
    const command = `pg_dump -h ${env.PGHOST} -p ${env.PGPORT} -U ${env.PGUSER} -d ${env.PGDATABASE} -f ${path}`;
    
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

const uploadToGCS = async ({ name, path }: { name: string; path: string }) => {
  await logToFile("Uploading backup to GCS...");

  const bucketName = env.GCS_BUCKET;

  const uploadOptions: UploadOptions = {
    destination: name,
  };

  const storage = new Storage({
    projectId: env.GOOGLE_PROJECT_ID,
    credentials: JSON.parse(env.SERVICE_ACCOUNT_JSON),
  });

  try {
    await storage.bucket(bucketName).upload(path, uploadOptions);
    await logToFile("Backup uploaded to GCS successfully.");
  } catch (error) {
    await logToFile(`Error uploading to GCS: ${error}`);
    throw error;
  }
};

const deleteFile = async (path: string) => {
  await logToFile("Deleting local backup file...");
  try {
    await fs.unlink(path);
    await logToFile("Local backup file deleted successfully.");
  } catch (error) {
    await logToFile(`Error deleting local file: ${error}`);
  }
};

export const backup = async () => {
  await logToFile("Initiating DB backup process...");

  const timestamp = new Date().toISOString().replace(/[:.]+/g, "-");
  const filename = `backup-${timestamp}.sql`;
  const filepath = `/tmp/${filename}`;

  try {
    // Set PGPASSWORD environment variable for pg_dump
    process.env.PGPASSWORD = env.PGPASSWORD;

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
    // Clear PGPASSWORD environment variable
    delete process.env.PGPASSWORD;
    await deleteFile(filepath);
  }

  await logToFile("DB backup process completed.");
};
