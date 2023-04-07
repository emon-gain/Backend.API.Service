const shell = require('shelljs');
shell.exec('babel src --out-dir dist --copy-files');
console.log('\n');
