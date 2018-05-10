var isDevelopment = process.env.NODE_ENV === 'development';

// アプリケーション作成用のモジュールを読み込み
const electron = require('electron');
const app = electron.app;
const BrowserWindow = electron.BrowserWindow;
const fs = require('fs-extra');
const constant = require('./config/constant');

// メインウィンドウ
var mainWindow = null;
var connect;

if (isDevelopment) {
  connect = require('electron-connect').client;
}

var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var webApp = express();
var port = 3456;

// view engine setup
webApp.set('views', path.join(__dirname, 'app/views'));
webApp.set('view engine', 'ejs');

webApp.use(logger('dev'));
webApp.use(bodyParser.json());
webApp.use(bodyParser.urlencoded({ extended: false }));
webApp.use(cookieParser());
webApp.use(express.static(path.join(__dirname, 'public')));
webApp.use(express.static(path.join(__dirname, 'vendor')));

// Routing
var openFileName = null;
var openFileHandler = function(event, openPath) {
  event.preventDefault();
  var rtadir = constant.rtadir;
  var savePath = path.join(rtadir, path.basename(openPath));
  fs.mkdirsSync(rtadir);
  fs.createReadStream(openPath).pipe(fs.createWriteStream(savePath));

  openFileName = path.basename(savePath, path.extname(savePath));
};
app.on('open-file', openFileHandler);

var index = require('./app/controllers/index');
var ptl = require('./app/controllers/ptl');
var dashboard = require('./app/controllers/dashboard');
var content = require('./app/controllers/content');
webApp.use('/', index);
webApp.use('/ptl', ptl);
webApp.use('/dashboard', dashboard);
webApp.use('/content', content);

var server = webApp.listen(port, function () {
  console.log('app listening at http://%s:%s', server.address().host, server.address().port);
});


function createWindow () {
  // メインウィンドウを作成します
  mainWindow = new BrowserWindow({width: 1600, height: 900});

  if (openFileName != null) {
    mainWindow.loadURL('http://localhost:' + port + '/content/' + openFileName);
  } else {
    mainWindow.loadURL('http://localhost:' + port);
  }

  let $ = require('jquery');
  mainWindow.$ = $;

  // mainWindow.webContents.openDevTools();

  // メインウィンドウが閉じられたときの処理
  mainWindow.on('closed', function () {
    mainWindow = null;
  });
}

//  初期化が完了した時の処理
app.on('ready', createWindow);

// 全てのウィンドウが閉じたときの処理
app.on('window-all-closed', function () {
  // macOSのとき以外はアプリケーションを終了させます
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
// アプリケーションがアクティブになった時の処理(Macだと、Dockがクリックされた時）
app.on('activate', function () {
  /// メインウィンドウが消えている場合は再度メインウィンドウを作成する
  if (mainWindow === null) {
    createWindow();
  }
});
