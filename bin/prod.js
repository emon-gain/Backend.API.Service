const shell = require('shelljs');
const path = require('path');
const fs = require('fs');

const dist = path.resolve('dist');
if (fs.existsSync(dist)) {
  shell.exec('node dist');
} else {
  console.log('Build not found. Creating build file...\n');
  shell.exec('npm run build');
}
