import { decryptSecret } from '../../lib/decryption'

export const getEnvironmentVariable = async (envVar) =>
  process.env[envVar] ? process.env[envVar] : await decryptSecret(envVar)
