var width = 800;
var height = 1000;
var page = require('webpage').create(),
    loadInProgress = false,
    fs = require('fs');
page.viewportSize = {
    height: height,
    width: width
};
var htmlFiles = new Array();
console.log(fs.workingDirectory);
var curdir = fs.list(fs.workingDirectory);

// loop through files and folders
for (var i = 0; i < curdir.length; i++) {
    var fullpath = fs.workingDirectory + fs.separator + curdir[i];
    // check if item is a file
    if (fs.isFile(fullpath)) {
        // check that file is html
        if (fullpath.indexOf('.html') != -1) {
            // show full path of file
            console.log('File path: ' + fullpath);
            htmlFiles.push(fullpath);
        }
    }
}

console.log('Number of Html Files: ' + htmlFiles.length);

// output pages as PNG
var pageindex = 0;

var interval = setInterval(function() {
    if (!loadInProgress && pageindex < htmlFiles.length) {
        console.log("image " + (pageindex + 1));
        page.open(htmlFiles[pageindex]);
        //page.dpi=300;
    }
    if (pageindex == htmlFiles.length) {
        console.log("image render complete!");
        phantom.exit();
    }
}, 2000);

page.onLoadStarted = function() {
    loadInProgress = true;
    console.log('page ' + (pageindex + 1) + ' load started');
};

page.onLoadFinished = function() {
    loadInProgress = false;
    //page.evaluate(function(w, h) {
    //document.body.style.width = w + "px";
    //document.body.style.height = h + "px";
    //}, width, height);
    //page.clipRect = {top: 0, left: 0, width: width, height: height}; 
    //page.render("images/output" + ("0"+(pageindex + 1)).slice(-2) + ".png");
    page.render("images/output" + pageindex + ".png");
    console.log('page ' + (pageindex) + ' load finished');
    pageindex++;
}
