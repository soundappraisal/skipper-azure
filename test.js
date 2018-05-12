require('skipper-adapter-tests')({
    mocha: { timeout: 360000 }, 
    module: require('./'), 
    container:process.env.CONTAINER || 'skipper-adapter-tests', 
    key: process.env.KEY, secret: process.env.SECRET
});