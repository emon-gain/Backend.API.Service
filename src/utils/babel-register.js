if (!process.env.CI) {
    require('dotenv').config()
}

require('@babel/register')({
    configFile: './src/utils/babel.config.js'
})
require('@babel/polyfill')
