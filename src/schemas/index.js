import fs from 'fs';
import path from 'path';
import { mergeTypeDefs } from '@graphql-tools/merge';
import { loadFilesSync } from '@graphql-tools/load-files';
export const createMergedSchema = mergeTypeDefs(
  loadFilesSync(path.join(__dirname, './*.gql'))
);
