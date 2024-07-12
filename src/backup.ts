import { exec } from "child_process";
import { Storage, UploadOptions } from "@google-cloud/storage";
import { unlink } from "fs";

import { env } from "./env";

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

const dumpToFile = async (path: string) => {
  console.log("Dumping DB to file...");

  return new Promise((resolve, reject) => {
    const command = `pg_dump ${env.BACKUP_DATABASE_URL} | gzip > ${path}`;
    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.error(`exec error: ${error}`);
        reject({ error: JSON.stringify(error), stderr });
        return;
      }
      if (stderr) {
        console.error(`stderr: ${stderr}`);
      }
      if (stdout) {
        console.log(`stdout: ${stdout}`);
      }
      
      // Check if the file was created and has content
      exec(`ls -l ${path}`, (err, output) => {
        if (err) {
          console.error(`Failed to check file: ${err}`);
          reject(err);
        } else {
          console.log(`File details: ${output}`);
          resolve(undefined);
        }
      });
    });
  });
};

const deleteFile = async (path: string) => {
  console.log("Deleting file...");
  await new Promise((resolve, reject) => {
    unlink(path, (err) => {
      reject({ error: JSON.stringify(err) });
      return;
    });
    resolve(undefined);
  });
};

export const backup = async () => {
  console.log("Initiating DB backup...");

  let date = new Date().toISOString();
  const timestamp = date.replace(/[:.]+/g, "-");
  const filename = `${env.BACKUP_PREFIX}backup-${timestamp}.tar.gz`;
  const filepath = `/tmp/${filename}`;

  await dumpToFile(filepath);
  await uploadToGCS({ name: filename, path: filepath });
  await deleteFile(filepath);

  console.log("DB backup complete...");
};
