var wavesurfer;
var timeline;
var totalTime;
var currentTime;
var pxPerSec = 50;
var seconds = 1.00;
var muitSpRegions = [];
var RegionList = [];
var speakerColorList = [];
var spkRegionList = [];
var hasXmlFile = false;
var hasWaveFile = false;
var minPxPerSec = 50;
var waveformWidth;
var waveUrl;
var hasTable = false;
var isPlay = false;
var isMuted = false;
var isToggled = false;

var wavesurfer2;
var waveUrl2;
var timeline2;
var totalTime2;
var currentTime2;
var muitSpRegions2 = [];
var RegionList2 = [];
var speakerColorList2 = [];
var spkRegionList2 = [];
var hasTable2 = false;
var isPlay2 = false;
var isMuted2 = false;
var isToggled2 = false;

const colorArr = [
    'hsla(180,50%,50%,0.3)',
    'hsla(300, 90%, 30%, 0.2)',
    'hsla(200, 100%, 30%, 0.3)',
    'hsla(400, 100%, 30%, 0.3)',
    'hsla(120,65%,75%,0.3)',
    'hsla(250,65%,75%,0.3)',
    'hsla(120, 100 %, 50%, 0.3)',
    'hsla(350, 80%, 40%, 0.3)',
    'hsla(190, 100%, 60%, 0.3)'
];

function initWaveSurfer(url) {
    wavesurfer = WaveSurfer.create({
        container: '#waveform',
        waveColor: 'green',
        fillParent: false,
        scrollParent: true,
        minPxPerSec: 50,
        hideScrollbar: false,
        barHeight: 2,
        height: 200,
        responsive: true

    });
    wavesurfer.load(url);
    wavesurfer.on('ready', function() {
        totalTime = wavesurfer.getDuration();
        var wf = document.getElementById("waveform");
        var rt = wf.getBoundingClientRect();
        waveformWidth = rt.left + rt.right;
        timeline = Object.create(WaveSurfer.Timeline);
        timeline.init({
            wavesurfer: wavesurfer,
            container: '#waveform-timeline',
            // formatTimeCallback: formatTimeCallback(seconds, pxPerSec),
            // timeInterval: timeInterval(pxPerSec),
            // primaryLabelInterval: primaryLabelInterval(pxPerSec),
            // secondaryLabelInterval: secondaryLabelInterval(pxPerSec),
            primaryColor: 'red',
            secondaryColor: 'blue',
            primaryFontColor: 'red',
            secondaryFontColor: 'blue'
        });
        wavesurfer.enableDragSelection({});
        if (muitSpRegions.length < 1) {
            console.log("no data!");
            return;
        }
        createInfoDiv();
        createAnime();
        for (let i in muitSpRegions) {
            let tmp = muitSpRegions[i];
            let arrs = tmp.regions;
            for (let i in arrs) {
                let tmp = arrs[i];
                tmp.id = tmp.id + "-" + tmp.start;
                let reg = wavesurfer.addRegion(tmp);
                RegionList.push(reg);
            }
        }
    });

    wavesurfer.on('audioprocess', function() {});

    wavesurfer.on('finish', function() {
        clearInfo();
    });

    wavesurfer.on("region-in", function(region) {
        //render(region);           
        let id = region.id;
        let index = id.indexOf('-');
        let name = id.substring(0, index);
        let target = document.getElementById(name);
        let txt = name + " is speaking!!";
        $(target).html(txt);
        $(target).css("color", "red");
    });

    wavesurfer.on("region-out", function(region) {
        let id = region.id;
        let index = id.indexOf('-');
        let name = id.substring(0, index);
        let target = document.getElementById(name);
        let txt = name;
        $(target).html(txt);
        $(target).css("color", region.color);


    });

    wavesurfer.on('region-click', function(region, e) {
        console.log(region.start);
        console.log(region.end);
        e.stopPropagation();
        wavesurfer.play(region.start, region.end);
    });

} //end int wavessurfer

function initWaveSurfer2(url) {
    wavesurfer2 = WaveSurfer.create({
        container: '#waveform2',
        waveColor: 'pink',
        fillParent: false,
        scrollParent: true,
        minPxPerSec: 50,
        hideScrollbar: false,
        barHeight: 2,
        height: 200,
        responsive: true

    });
    wavesurfer2.load(url);
    wavesurfer2.on('ready', function() {
        totalTime2 = wavesurfer2.getDuration();
        var wf = document.getElementById("waveform2");
        var rt = wf.getBoundingClientRect();
        waveformWidth2 = rt.left + rt.right;
        timeline2 = Object.create(wavesurfer2.Timeline);
        timeline2.init({
            wavesurfer: wavesurfer2,
            container: '#waveform-timeline2',
            // formatTimeCallback: formatTimeCallback(seconds, pxPerSec),
            // timeInterval: timeInterval(pxPerSec),
            // primaryLabelInterval: primaryLabelInterval(pxPerSec),
            // secondaryLabelInterval: secondaryLabelInterval(pxPerSec),
            primaryColor: 'red',
            secondaryColor: 'blue',
            primaryFontColor: 'red',
            secondaryFontColor: 'blue'
        });
        wavesurfer2.enableDragSelection({});
        if (muitSpRegions2.length < 1) {
            console.log("no data!");
            return;
        }
        createInfoDiv2();
        createAnime2();
        for (let i in muitSpRegions2) {
            let tmp = muitSpRegions2[i];
            let arrs = tmp.regions;
            for (let i in arrs) {
                let tmp = arrs[i];
                tmp.id = tmp.id + "-" + tmp.start;
                let reg = wavesurfer2.addRegion(tmp);
                RegionList2.push(reg);
            }
        }
    });

    wavesurfer2.on('audioprocess', function() {});

    wavesurfer2.on('finish', function() {
        //clearInfo2();
    });

    wavesurfer2.on("region-in", function(region) {
        //render(region);           
        let id = region.id;
        let index = id.indexOf('-');
        let name = id.substring(0, index) ;
        let myName = name + ":2";
        let target = document.getElementById(myName);
        let txt = name + " is speaking!!";
        $(target).html(txt);
        $(target).css("color", "red");
    });

    wavesurfer2.on("region-out", function(region) {
        let id = region.id;
        let index = id.indexOf('-');
        let name = id.substring(0, index);
        let myName = name + ":2";
        let target = document.getElementById(myName);
        let txt = name;
        $(target).html(txt);
        $(target).css("color", region.color);
    });

    wavesurfer2.on('region-click', function(region, e) {
        console.log(region.start);
        console.log(region.end);
        e.stopPropagation();
        wavesurfer2.play(region.start, region.end);
    });

} //end int wavessurfer2

function myplay() {
    if (!isPlay) {
        $("#cplay").removeClass('fa-play-circle');
        $("#cplay").addClass('fa-pause-circle');
    } else {
        $("#cplay").removeClass('fa-pause-circle');
        $("#cplay").addClass('fa-play-circle');
    }
    isPlay = !isPlay;
    if (!wavesurfer.params.scrollParent) {
        wavesurfer.toggleScroll();
    }
    clearInfo();
    wavesurfer.playPause();
}

function myplay2() {
    if (!isPlay2) {
        $("#cplay2").removeClass('fa-play-circle');
        $("#cplay2").addClass('fa-pause-circle');
    } else {
        $("#cplay2").removeClass('fa-pause-circle');
        $("#cplay2").addClass('fa-play-circle');
    }
    isPlay2 = !isPlay2;
    if (!wavesurfer2.params.scrollParent) {
        wavesurfer2.toggleScroll();
    }
    //clearInfo2();
    wavesurfer2.playPause();
}

function doStop() {
    wavesurfer.stop();
}

function doStop2() {
    wavesurfer2.stop();
}


function toggleMute() {
    if (!isMuted) {
        $("#cmute").removeClass('fa-volume-off');
        $("#cmute").addClass('fa-volume-up');
    } else {
        $("#cmute").removeClass('fa-volume-up');
        $("#cmute").addClass('fa-volume-off');
    }
    isMuted = !isMuted;
    wavesurfer.toggleMute();
    let muteBtn = $("#mute");
    if (wavesurfer.isMuted) {
        muteBtn.prop('title', 'not muted');
        //muteBtn.html('SetToNoMute');
        $("#cmute").removeClass('fa-volume-mute');
    } else {
        // muteBtn.html('SetToMute');
        muteBtn.prop('title', 'muted');
    }
}

function toggleMute2() {
    if (!isMuted2) {
        $("#cmute2").removeClass('fa-volume-off');
        $("#cmute2").addClass('fa-volume-up');
    } else {
        $("#cmute2").removeClass('fa-volume-up');
        $("#cmute2").addClass('fa-volume-off');
    }
    isMuted2 = !isMuted2;
    wavesurfer2.toggleMute();
    let muteBtn = $("#mute2");
    if (wavesurfer2.isMuted) {
        muteBtn.prop('title', 'not muted');
        //muteBtn.html('SetToNoMute');
        $("#cmute2").removeClass('fa-volume-mute');
    } else {
        // muteBtn.html('SetToMute');
        muteBtn.prop('title', 'muted');
    }
}

function myToggle() {
    if (!isToggled) {
        $("#cttab").removeClass('fa-toggle-on');
        $("#cttab").addClass('fa-toggle-off');
    } else {
        $("#cttab").removeClass('fa-toggle-off');
        $("#cttab").addClass('fa-toggle-on');
    }
    isToggled = !isToggled;
}

function myToggle2() {
    if (!isToggled2) {
        $("#cttab2").removeClass('fa-toggle-on');
        $("#cttab2").addClass('fa-toggle-off');
    } else {
        $("#cttab2").removeClass('fa-toggle-off');
        $("#cttab2").addClass('fa-toggle-on');
    }
    isToggled2 = !isToggled2;
}

function clearAll() {
    $('#audioFile').val('');
    muitSpRegions = [];
    RegionList = [];
    speakerColorList = [];
    spkRegionList = [];
    hasXmlFile = false;
    hasWaveFile = false;
    waveUrl = "";
    hasTable = false;
    isPlay = false;
    isMuted = false;
    isToggled = false;
    wavesurfer.destroy();

    muitSpRegions2 = [];
    RegionList2 = [];
    speakerColorList2 = [];
    spkRegionList2 = [];
    waveUrl2 = "";
    hasTable2 = false;
    isPlay2 = false;
    isMuted2 = false;
    isToggled2 = false;
    wavesurfer2.destroy();
}

function toggleScroll() {
    wavesurfer.toggleScroll();
}

function toggleScroll2() {
    wavesurfer2.toggleScroll();
}

function playRegion() {
    //var region = $(".wavesurfer-region"); 
    for (let i = 0; i < muitSpRegions.length; i++) {
        var test = muitSpRegions[i];
        var name = test.name;
        var regins = test.regions;
        var rg = regins[0];
        for (let j = 0; j < regins.length; j++) {
            var rg = regins[i];
            var st = rg.start;
            var ed = rg.end;
            wavesurfer.play(st, ed);
            wavesurfer.playPause();
        }
    }
}

async function doPlayBytutton(e) {
    await playByButton(e);
}

function playByButton(ev) {
    wavesurfer.playPause();
    let row = $(ev).parent().parent().prevAll().length;
    let col = $(ev).parent().prevAll().length;
    let tab = document.getElementById("spkTab").childNodes[0];
    let spname = tab.rows[row].cells[0].innerHTML;
    let value = tab.rows[0].cells[col].innerHTML;
    let arr = value.split("-");
    let start = arr[0];
    let end = arr[1];
    let delay = ms => new Promise(resolve => setTimeout(resolve, ms));
    (async function loop() {
        for (let i = 0; i < muitSpRegions.length; i++) {
            let test = muitSpRegions[i];
            let name = test.name;
            if (spname === name) {
                let regins = test.regions;
                for (let j = 0; j < regins.length; j++) {
                    let rg = regins[j];
                    let st = rg.start;
                    if (st === start) {
                        if (wavesurfer.isPlaying()) {
                            wavesurfer.stop();
                        }
                        //wavesurfer.seekTo(start/totalTime);
                        wavesurfer.play(start, end);
                        await delay(500);

                    }
                }
            }
        }
    })();
}


function createTable() {
    if (hasTable) {
        alert("Speaker table is exist, skip!");
        return;
    }
    hasTable = true;
    var container = $("#table_container");
    let cap = "<h2>Speaker Timeline Info</>";
    $("#tabTitle").html(cap);

    var table = $("<table border=\"1\">");
    //let cap = $("<caption>Speaker List</caption>");
    //cap.attr("caption-side", "top");
    //cap.appendTo(table);      
    table.appendTo($("#spkTab"));

    let tr0 = $("<tr></tr>");
    tr0.appendTo(table);
    let td0 = $("<td>" + "Speaker" + "</td>");
    td0.attr('noWarp', 'noWrap');
    td0.css("color", "black");
    td0.appendTo(tr0);
    for (let i = 0; i < spkRegionList.length; i++) {
        let reg = spkRegionList[i];
        let txt = reg.start + "-" + reg.end;
        let td = $("<td></td>");
        td.html(txt);
        td.addClass('mycell');
        td.css("color", "blue");
        td.appendTo(tr0);
    }
    let rowCount = speakerColorList.length;
    for (let j = 0; j < rowCount; j++) {
        let name = speakerColorList[j].id;
        let ftr = $("<tr></tr>");
        let ftd = $("<td>" + name + "</td>");
        ftd.attr('noWarp', 'noWrap');
        ftd.css("color", "red");
        ftd.appendTo(ftr);
        for (let k = 0; k < spkRegionList.length; k++) {
            let one = spkRegionList[k];
            let td = $("<td></td>");
            td.css("white-space", "nowrap");
            if (one.id === name) {
                let txt = one.id + ':' + one.start + '-' + one.end;
                // let btn = $("<button type='button' class='btn btn-outline-success'>Play</button>"); 
                // btn.attr('title',txt);
                // btn.attr('onClick', 'doPlayBytutton(this);');                   
                // td.append(btn);
                let myad = new Audio(waveUrl + '#t=' + one.start + ',' + one.end);
                myad.controls = true;
                $(myad).css({
                    'width': '100'
                });
                // $(myad).attr('title',txt); 
                td.attr('title', txt);
                td.append(myad);

            }
            td.appendTo(ftr);
            ftr.appendTo(table);
        }
    }

    $('.collapse').collapse();
} //end createTable

function createTable2() {
    if (hasTable2) {
        alert("Speaker table2 is exist, skip!");
        return;
    }
    hasTable2 = true;
    var container = $("#table_container2");
    let cap = "<h2>Speaker Timeline Info</>";
    $("#tabTitle2").html(cap);

    var table = $("<table border=\"1\">");
    //let cap = $("<caption>Speaker List</caption>");
    //cap.attr("caption-side", "top");
    //cap.appendTo(table);      
    table.appendTo($("#spkTab2"));

    let tr0 = $("<tr></tr>");
    tr0.appendTo(table);
    let td0 = $("<td>" + "Speaker" + "</td>");
    td0.attr('noWarp', 'noWrap');
    td0.css("color", "black");
    td0.appendTo(tr0);
    for (let i = 0; i < spkRegionList2.length; i++) {
        let reg = spkRegionList2[i];
        let txt = reg.start + "-" + reg.end;
        let td = $("<td></td>");
        td.html(txt);
        td.addClass('mycell');
        td.css("color", "blue");
        td.appendTo(tr0);
    }
    let rowCount = speakerColorList2.length;
    for (let j = 0; j < rowCount; j++) {
        let name = speakerColorList2[j].id;
        let ftr = $("<tr></tr>");
        let ftd = $("<td>" + name + "</td>");
        ftd.attr('noWarp', 'noWrap');
        ftd.css("color", "red");
        ftd.appendTo(ftr);
        for (let k = 0; k < spkRegionList2.length; k++) {
            let one = spkRegionList2[k];
            let td = $("<td></td>");
            td.css("white-space", "nowrap");
            if (one.id === name) {
                let txt = one.id + ':' + one.start + '-' + one.end;
                // let btn = $("<button type='button' class='btn btn-outline-success'>Play</button>"); 
                // btn.attr('title',txt);
                // btn.attr('onClick', 'doPlayBytutton(this);');                   
                // td.append(btn);
                let myad = new Audio(waveUrl2 + '#t=' + one.start + ',' + one.end);
                myad.controls = true;
                $(myad).css({
                    'width': '100'
                });
                // $(myad).attr('title',txt); 
                td.attr('title', txt);
                td.append(myad);

            }
            td.appendTo(ftr);
            ftr.appendTo(table);
        }
    }

    $('.collapse2').collapse();
} //end createTable2

function formatTimeCallback(seconds, pxPerSec) {
    seconds = Number(seconds);
    var minutes = Math.floor(seconds / 60);
    seconds = seconds % 60;
    // fill up seconds with zeroes
    var secondsStr = Math.round(seconds).toString();
    if (pxPerSec >= 25 * 10) {
        secondsStr = seconds.toFixed(2);
    } else if (pxPerSec >= 25 * 1) {
        secondsStr = seconds.toFixed(1);
    }

    if (minutes > 0) {
        if (seconds < 10) {
            secondsStr = '0' + secondsStr;
        }
        return `${minutes}:${secondsStr}`;
    }
    return secondsStr;
}

function timeInterval(pxPerSec) {
    var retval = 1;
    if (pxPerSec >= 25 * 100) {
        retval = 0.01;
    } else if (pxPerSec >= 25 * 40) {
        retval = 0.025;
    } else if (pxPerSec >= 25 * 10) {
        retval = 0.1;
    } else if (pxPerSec >= 25 * 4) {
        retval = 0.25;
    } else if (pxPerSec >= 25) {
        retval = 1;
    } else if (pxPerSec * 5 >= 25) {
        retval = 5;
    } else if (pxPerSec * 15 >= 25) {
        retval = 15;
    } else {
        retval = Math.ceil(0.5 / pxPerSec) * 60;
    }
    return retval;
}

function primaryLabelInterval(pxPerSec) {
    var retval = 1;
    if (pxPerSec >= 25 * 100) {
        retval = 10;
    } else if (pxPerSec >= 25 * 40) {
        retval = 4;
    } else if (pxPerSec >= 25 * 10) {
        retval = 10;
    } else if (pxPerSec >= 25 * 4) {
        retval = 4;
    } else if (pxPerSec >= 25) {
        retval = 1;
    } else if (pxPerSec * 5 >= 25) {
        retval = 5;
    } else if (pxPerSec * 15 >= 25) {
        retval = 15;
    } else {
        retval = Math.ceil(0.5 / pxPerSec) * 60;
    }
    return retval;
}

function secondaryLabelInterval(pxPerSec) {
    // draw one every 10s as an example
    return Math.floor(10 / timeInterval(pxPerSec));
}

function handerDragAndDrop() {
    let obj = $("inputmyfile");
    obj.on('dragenter', function(e) {
        e.stopPropagation();
        e.preventDefault();
        $(this).css('border', '1px solid #0B85A1');
        $(this).width(100).height(2);
    });
    obj.on('dragover', function(e) {
        e.stopPropagation();
        e.preventDefault();
        e.dataTransfer.dropEffect = 'copy';
    });

    obj.on('dragleave', function(e) {
        e.stopPropagation();
        e.preventDefault();
        e.dataTransfer.dropEffect = 'copy';
    });

    obj.on('drop', function(e) {
        $(this).css('border', '1px dotted #0B85A1');
        $(this).width(100).height(2);
        e.preventDefault();
        let file = e.dataTransfer.files[0];
        let url = URL.createObjectURL(file);
        initWaveSurfer(url);
    });
}
handerDragAndDrop();

function clear() {
    var inputObj = document.getElementById("#audioFile");
    inputObj.value = '';
}

function createRegions(start, end, color) {
    let regionObj = {};
    regionObj["start"] = start;
    regionObj["end"] = end;
    regionObj["color"] = color;
    return regionObj;
}

function createSpRegions(name, color, region) {
    let spRegions = {};
    spRegions["regions"] = [];
    spRegions["color"] = color;
    spRegions["regions"].push(region);
    return spRegions;
}

function addSpRegions(spRegions, regions) {
    if (!spRegions["regions"] || spRegions["regions"].length < 1) {
        spRegions["regions"] = [];
    }
    if (region.length < 1) {
        spRegions["regions"].push(regionregions);
    } else {
        spRegions["regions"].concat(regions);
    }
}

function createAnime() {
    let parentDiv = document.getElementById("myAnime");
    for (let i in muitSpRegions) {
        let mydiv = $('<div></div>');
        mydiv.attr('id', muitSpRegions[i].name + "anime");
        mydiv.addClass('oneanime');
        let iobj = $('<i class="fas fa-smile  fa-5x"></i>');
        iobj.css('color', muitSpRegions[i].regions[0].color);
        iobj.appendTo(mydiv);
        mydiv.appendTo(parentDiv);
    }
}

function createAnime2() {
    let parentDiv = document.getElementById("myAnime2");
    for (let i in muitSpRegions2) {
        let mydiv = $('<div></div>');
        mydiv.attr('id', muitSpRegions2[i].name + "anime"  + ":2");
        mydiv.addClass('oneanime');
        let iobj = $('<i class="fas fa-smile  fa-5x"></i>');
        iobj.css('color', muitSpRegions2[i].regions[0].color);
        iobj.appendTo(mydiv);
        mydiv.appendTo(parentDiv);
    }
}

function createInfoDiv() {
    let parentDiv = document.getElementById("audioInfo");
    totalTime = wavesurfer.getDuration();
    for (let i in muitSpRegions) {
        let mydiv = $('<div></div>');
        mydiv.attr('id', muitSpRegions[i].name);
        mydiv.attr('fontSize', 26);
        mydiv.attr('font-weight', "bold");
        mydiv.addClass('info');
        mydiv.text(muitSpRegions[i].name);
        let arrs = muitSpRegions[i].regions;
        let cr = arrs[0];
        mydiv.css("color", arrs[0].color);
        mydiv.appendTo(parentDiv);
        mydiv.on("click", function(e) {
            wavesurfer.seekTo(0);
            let objTarget = e.target;
            e.stopPropagation();
            let delay = ms => new Promise(resolve => setTimeout(resolve, ms));
            (async function loop() {
                for (let j in RegionList) {
                    // await delay(500);
                    let reg = RegionList[j];
                    let index = reg.id.indexOf('-');
                    let name = reg.id.substring(0, index);
                    if (!(name === objTarget.id)) {
                        continue;
                    }
                    reg.play();
                    await delay(1000);
                }
            })();
        });
    }
}

function createInfoDiv2() {
    let parentDiv = document.getElementById("audioInfo2");
    totalTime = wavesurfer2.getDuration();
    for (let i in muitSpRegions2) {
        let mydiv = $('<div></div>');        
        mydiv.attr('id', muitSpRegions2[i].name + ":2");
        mydiv.attr('fontSize', 26);
        mydiv.attr('font-weight', "bold");
        mydiv.addClass('info');
        mydiv.text(muitSpRegions2[i].name);
        let arrs = muitSpRegions2[i].regions;
        let cr = arrs[0];
        mydiv.css("color", arrs[0].color);
        mydiv.appendTo(parentDiv);
        mydiv.on("click", function(e) {
            wavesurfer2.seekTo(0);
            let objTarget = e.target;
            e.stopPropagation();
            let delay = ms => new Promise(resolve => setTimeout(resolve, ms));
            (async function loop() {
                for (let j in RegionList2) {
                    // await delay(500);
                    let reg = RegionList2[j];
                    let index = reg.id.indexOf('-');
                    let name = reg.id.substring(0, index) + ":2";
                    if (!(name === objTarget.id)) {
                        continue;
                    }
                    reg.play();
                    await delay(1000);
                }
            })();
        });
    }
}




function createSpkInfo() {
    clearInfo();
    if (wavesurfer.isPlaying()) {
        wavesurfer.stop();
    }
    if (wavesurfer.params.scrollParent) {
        wavesurfer.toggleScroll();
    }
    let temp = wavesurfer.params.scrollParent;
    for (let j in RegionList) {
        let one = RegionList[j];
        render(one);
    }
}

function createSpkInfo2() {
    clearInfo2();
    if (wavesurfer2.isPlaying()) {
        wavesurfer2.stop();
    }
    if (wavesurfer2.params.scrollParent) {
        wavesurfer2.toggleScroll();
    }
    let temp = wavesurfer2.params.scrollParent;
    for (let j in RegionList2) {
        let one = RegionList2[j];
        render(one);
    }
}


function render(region) {
    canvas = document.getElementById("audio_canvas");
    context = canvas.getContext("2d");
    // context.translate(0, 0);  
    context.width = totalTime * 50;
    waveformWidth;
    var baseH = 0;
    let drawH = 50;
    context.beginPath();
    context.fillStyle = region.color;
    let st = region.start;
    let ed = region.end;
    context.fillRect(st * 50, baseH, (ed - st) * 50, drawH);
    context.save();
    context.closePath();
    context.beginPath();
    context.fillStyle = "green";
    context.font = '24pt Calibri';
    context.fillText(st + ":" + ed, st * 50 + ((ed - st) * 50) / 2 - 10, drawH - 12);
    context.closePath();
    context.restore();
}

function clearInfo() {
    const infoDiv = document.getElementById("audioInfo");
    for (let i = 0; i < infoDiv.children.length; i++) {
        let subDiv = infoDiv.children[i];
        let name = $(subDiv).attr('id');
        subDiv.innerHTML = name;
    }
}

function clearInfo2() {
    const infoDiv = document.getElementById("audioInfo2");
    for (let i = 0; i < infoDiv.children.length; i++) {
        let subDiv = infoDiv.children[i];
        let name = $(subDiv).attr('id');
        let printName =name.substr(0, name.length -2);
        subDiv.innerHTML = printName;
    }
}

function Region(id, start, end, color) {
    this.id = id;
    this.start = start;
    this.end = end;
    this.color = color;
}

function SpeakerColor(name, color) {
    this.id = name;
    this.color = color;
}

function SpeekerRegions(name, regions) {
    this.name = name;
    if (regions.length > 0) {
        this.regions = regions;
    } else {
        this.regions = [];
    }
}

function readXmlNew(fileList, evt) {
    if (!fileList.length) {
        alert('File is not selected.');
        return;
    }
    let fileToLoad = evt.target.files[0];
    if (fileToLoad) {
        let reader = new FileReader();
        reader.onload = function(fileLoadedEvent) {
            let data = fileLoadedEvent.target.result;
            let x2js = new X2JS();
            let jsonTmp = x2js.xml_str2json(data);
            let spList = jsonTmp.speakerList;
            let set = new Set();
            for (let i = 0; i < spList.speaker.length; i++) {
                set.add(spList.speaker[i]._name);
            }
            let colorIndex = 0;
            set.forEach(x => {
                let spkClr = new SpeakerColor(x, colorArr[colorIndex]);
                speakerColorList.push(spkClr);
                colorIndex++;
            });
            for (let i = 0; i < spList.speaker.length; i++) {
                let temp = spList.speaker[i];
                let name = temp._name;
                let start = temp._start;
                let end = temp._end;
                spkRegionList.push(new Region(name, start, end, getColor(name)));
            }

        };
        reader.readAsText(fileToLoad, 'UTF-8');
    }
} //end readXmlNew  

function readXmlNew2(file, evt) {
    let fileToLoad = evt.target.files[0];
    if (fileToLoad) {
        let reader = new FileReader();
        reader.onload = function(fileLoadedEvent) {
            let data = fileLoadedEvent.target.result;
            let x2js = new X2JS();
            let jsonTmp = x2js.xml_str2json(data);
            let spList = jsonTmp.speakerList;
            let set = new Set();
            for (let i = 0; i < spList.speaker.length; i++) {
                set.add(spList.speaker[i]._name);
            }
            let colorIndex = 0;
            set.forEach(x => {
                let spkClr = new SpeakerColor(x, colorArr[colorIndex]);
                speakerColorList2.push(spkClr);
                colorIndex++;
            });
            for (let i = 0; i < spList.speaker.length; i++) {
                let temp = spList.speaker[i];
                let name = temp._name;
                let start = temp._start;
                let end = temp._end;
                spkRegionList2.push(new Region(name, start, end, getColor(name)));
            }

        };
        reader.readAsText(fileToLoad, 'UTF-8');
    }
} //end readXmlNew2 

function getColor(id) {
    for (let one in speakerColorList) {
        if (speakerColorList[one].name === id) {
            return speakerColorList[one].color;
        }
    }
}

function readXml(fileList, evt) {
    if (!fileList.length) {
        alert('File is not selected.');
        return;
    }
    let fileToLoad = evt.target.files[0];
    if (fileToLoad) {
        let reader = new FileReader();
        reader.onload = function(fileLoadedEvent) {
            let data = fileLoadedEvent.target.result;
            let x2js = new X2JS();
            let jsonTmp = x2js.xml_str2json(data);
            let spList = jsonTmp.speakerList;
            let set = new Set();
            for (let i = 0; i < spList.speaker.length; i++) {
                set.add(spList.speaker[i]._name);
            }
            let colorIndex = 0;
            set.forEach(x => {
                var myRegions = [];
                for (let i = 0; i < spList.speaker.length; i++) {
                    if (spList.speaker[i]._name === x) {
                        let temp = spList.speaker[i];
                        let name = temp._name;
                        let start = temp._start;
                        let end = temp._end;
                        myRegions.push(new Region(name, start, end, colorArr[colorIndex]));
                        myRegions.sort();
                    }
                }
                let spRegion = new SpeekerRegions(x, myRegions);
                muitSpRegions.push(spRegion);
                colorIndex++;
            });
            //alert(muitSpRegions.length);
            showSpkNum(muitSpRegions.length);

            //$.showModal({title: "info", body: ""});
        };
        reader.readAsText(fileToLoad, 'UTF-8');
    }
} //end readXml  

function readXml2(file, evt) {
    let fileToLoad = evt.target.files[0];
    if (fileToLoad) {
        let reader = new FileReader();
        reader.onload = function(fileLoadedEvent) {
            let data = fileLoadedEvent.target.result;
            let x2js = new X2JS();
            let jsonTmp = x2js.xml_str2json(data);
            let spList = jsonTmp.speakerList;
            let set = new Set();
            for (let i = 0; i < spList.speaker.length; i++) {
                set.add(spList.speaker[i]._name);
            }
            var colorIndex = 0;
            set.forEach(x => {
                let myRegions = [];
                for (let i = 0; i < spList.speaker.length; i++) {
                    if (spList.speaker[i]._name === x) {
                        let temp = spList.speaker[i];
                        let name = temp._name;
                        let start = temp._start;
                        let end = temp._end;
                        myRegions.push(new Region(name, start, end, colorArr[colorIndex]));
                        myRegions.sort();
                    }
                }
                let spRegion = new SpeekerRegions(x, myRegions);
                muitSpRegions2.push(spRegion);
                colorIndex++;
            });
            //alert(muitSpRegions.length);
            showSpkNum2(muitSpRegions2.length);

            //$.showModal({title: "info", body: ""});
        };
        reader.readAsText(fileToLoad, 'UTF-8');
    }
} //end readXml2  


function showSpkNum(num) {
    let delay = ms => new Promise(resolve => setTimeout(resolve, ms));
    (async function show() {
        let maru = document.getElementById("maru");
        $(maru).addClass("maru");
        //let spknum = document.getElementById("spknum");
        // $(spknum).addClass("letter3");
        let txt = "Speaker:" + num;
        $(maru).html(txt);
        await delay(1000);
        $(maru).removeClass("maru");
        //$(spknum).removeClass("spknum");
        $(maru).html('');
    })();
}

function showSpkNum2(num) {
    let delay = ms => new Promise(resolve => setTimeout(resolve, ms));
    (async function show() {
        let maru = document.getElementById("maru2");
        $(maru).addClass("maru");
        //let spknum = document.getElementById("spknum");
        // $(spknum).addClass("letter3");
        let txt = "Speaker:" + num;
        $(maru2).html(txt);
        await delay(1000);
        $(maru2).removeClass("maru");
        //$(spknum).removeClass("spknum");
        $(maru2).html('');
    })();
}

function readJson(evt) {
    var fileToLoad = evt.target.files[0];
    if (fileToLoad) {
        var reader = new FileReader();
        var fileName = fileToLoad.name;
        reader.onload = function(fileLoadedEvent) {
            var data = fileLoadedEvent.target.result;
            var json = JSON.parse(data);
            //same code go to here
        };
        reader.readAsText(fileToLoad, 'UTF-8');
    }
}

function doGetwaveDataAjx() {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', 'http://url', true);
    xhr.responseType = 'arraybuffer';
    xhr.onload = function(e) {
        if (this.status == 200) {
            // Note: .response instead of .responseText
            // var blob = new Blob([this.response], { type: 'image/png' });
            var uInt8Array = new Uint8Array(this.response);
        }
    };
    xhr.send();
}

function doGetXmlAjax(fileName) {
    var sendUrl = "?";
    $.ajax({
        type: "GET",
        url: sendUrl,
        timeout: 2000,
        dataType: "text",
        cache: false,
        async: false,
        success: function(data, status) {
            if (data == "" || data == null || data == undefined) {
                return;
            }
            updateVerifyJobResult(data);
        },
        error: function(XMLHttpRequest, status, errorThrown) {
            console.log('Can not to get verify job results, Reason: ' + errorThrown);
        }
    });
}