#!/bin/bash
npm run build
pm2 start pm2-config.json
