import { mergeResolvers } from '@graphql-tools/merge';
import { loadFilesSync } from '@graphql-tools/load-files';
import path from 'path';

console.log(loadFilesSync(path.join(__dirname, './*/index.js')));
export const createMergedResolvers = mergeResolvers(
  loadFilesSync(path.join(__dirname, './*/index.js'))
);
