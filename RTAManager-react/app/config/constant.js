const path = require('path');
var home = process.env[process.platform == "win32" ? "USERPROFILE" : "HOME"];
var rtadir = path.join(home, 'rta/library');
exports.rtadir = rtadir;

