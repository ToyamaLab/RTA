const crypto = require('crypto');
const algorithm = 'aes-256-ctr';
const passphrase = "M5pHUfys";

exports.encrypt = (text) => {
  var cipher = crypto.createCipher(algorithm, passphrase)
    var crypted = cipher.update(text,'utf8','base64')
    crypted += cipher.final('base64');
    return crypted;
  }

exports.decrypt = (text) => {
    var decipher = crypto.createDecipher(algorithm, passphrase)
  var dec = decipher.update(text,'base64','utf8')
  dec += decipher.final('utf8');
  return dec;
}

