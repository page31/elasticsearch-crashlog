require(['elasticsearch'], function(elasticsearch) {
    var client = new elasticsearch.Client({
        host: 'localhost:9200'});

    var productList = $('#product');
    var versionList = $('#version');
    var currentVersion = null;
    var histogram_data = {};

    var ratioData = [];
    var countData =  {'launch' : [], 'crash' : []};
    var ratioChart = new CanvasJS.Chart('ratio-chart', {
        title : {text: 'Crash Ratio'},
        axisX: {
            interlacedColor: "#F0F8FF"
        },
        data: [{
            type: 'spline',
            dataPoints : ratioData
        }]
    });

    var countChart = new CanvasJS.Chart('count-chart', {
        title : {text : 'Launch / Crash Count'},
        axisX: {
            interlacedColor: "#F0F8FF"
        },
        data: [{
                type : 'area',
                showInLegend: true,
                legendText: 'Launches',
                dataPoints: countData.launch,
            },
            {
                type: 'area',
                showInLegend: true,
                legendText: 'Crashes',
                dataPoints : countData.crash
            }
        ]
    });

    var crashPoints = [];
    var crashPointChart = new CanvasJS.Chart('crash-point-chart', {
        title : {text : 'Hot Crashes'},
        data: [{
            type: 'pie',
            showInLegend: true,
            dataPoints: crashPoints
        }]
    });
    ratioChart.render();
    countChart.render();
    var DATA_TYPE_CRASH = 'crash';
    var DATA_TYPE_LAUNCH = 'launch';
    var DATA_TYPE_RATIO = 'ratio';
    var intervals = {
        '1s' : 1000,
        '1m' : 60 * 1000,
        '1h' : 60 * 60 *1000,
        '1d' : 24 * 60 * 60 * 1000
    };
    var currentInterval = '1m';

    Array.prototype.pushAll = function (arr) {
        this.push.apply(this, arr);
    };

    Array.prototype.clear = function() {
        this.splice(0, this.length);
    }

    Array.prototype.replace = function(arr) {
        this.clear();
        this.pushAll(arr);
    }

    Array.prototype.shiftUntil = function(limit) {
        if (this.length > limit) {
            this.splice(0, this.length - limit);
        }
    }

    function reset() {
        histogram_data[DATA_TYPE_CRASH] = [];
        histogram_data[DATA_TYPE_LAUNCH] = [];
        histogram_data[DATA_TYPE_RATIO] = [];
    }

    productList.change(function() {
        var selected = $(this).find('option:selected');
        var versions = JSON.parse(selected.data('versions'));
        versionList.empty();
        for (var versionName in versions) {
            var option = $('<option/>');
            option.text(versionName);
            option.val(versionName);
            option.data('types', JSON.stringify(versions[versionName]));
            versionList.append(option);
        };
        versionList.change();
    });

    versionList.change(function() {
        currentVersion = getSelectedVersion();
        refresh();
    });

    function getSelectedVersion() {
        return {
            'product' : productList.val(),
            'types' : JSON.parse(versionList.find('option:selected')
                                            .data('types')),
            'version' : versionList.val()
        }
    }

    function fillVersions(data) {
        productList.empty();
        for(var p in data) {
            var option = $('<option/>');
            option.text(p);
            option.val(p);
            option.data('versions', JSON.stringify(data[p]));
            productList.append(option);
        }
        productList.change();
    }

    function searchTerms(field, index, type) {
        return client.search({'index' : index,
            'type': type,
            'body': {
                    query: {
                        match_all: {

                        }
                    },
                    facets: {
                        'terms': {
                            terms: {
                                'field': field,
                                'order': count,
                                'size': 10
                            }
                        }
                    }
                }
            });
    }

    function searchHistogram(field, interval, index, type) {
        return client.search({'index' : index,
            'type' : type,
            'searchType' : 'count',
            'body' : {
                    query : {
                        match_all : {

                        }
                    },
                    facets: {
                        '0' : {
                            date_histogram : {
                                'field' : field,
                                'interval' : interval
                            },
                            global: true
                        }
                    }
                }
        });
    }

    function updateChartData(type, data) {
        histogram_data[type] = data;
        histogram_data[type].shiftUntil(180);
    }

    function render() {
        var intervalValue = intervals[currentInterval];
        var launchTimes = histogram_data[DATA_TYPE_LAUNCH].map(function(item) {
            return item.time / intervalValue;
        });
        var crashTimes = histogram_data[DATA_TYPE_CRASH].map(function(item) {
            return item.time / intervalValue;
        });
        ratioData.clear();
        for (var i = 0; i < launchTimes.length; i++) {
            var time = launchTimes[i];
            var crashData = crashTimes.indexOf(time);
            var ratio = 0;
            if (crashData >= 0 && histogram_data[DATA_TYPE_LAUNCH][i].count > 0) {
                ratio = histogram_data[DATA_TYPE_CRASH][crashData].count / histogram_data[DATA_TYPE_LAUNCH][i].count;
            }
            var point = {x : new Date(time * intervalValue), y : ratio};
            ratioData.push(point);
        };

        countData.launch.replace(histogram_data[DATA_TYPE_LAUNCH].map(function(item) {
            return {x: new Date(item.time), y : item.count};
        }));

        countData.crash.replace(histogram_data[DATA_TYPE_CRASH].map(function(item) {
            return {x: new Date(item.time), y : item.count};
        }));

        ratioChart.render();
        countChart.render();
        reset();
    }

    function SearchPipline() {
        this.pipline = [];
        this.currentIndex = 0;
        this.timer = null;
        this.retryTimes = 0;
    }

    SearchPipline.prototype = {
        add: function(search, callback) {
            this.pipline.push({'search': search, 'callback': callback});
        },
        reset: function() {
            this.pause();
            this.currentIndex = 0;
        },
        step: function(delay) {
            if (delay > 0) {
                this.pause();
                thisObj = this;
                this.timer = window.setTimeout(function(){
                    thisObj.step(0);
                }, delay);
                return;
            } else {
                this.pause();
            }
            if (this.currentIndex == this.pipline.length)
                this.currentIndex = 0;
            var currentStep = this.pipline[this.currentIndex];
            var task = currentStep.search();
            var thisObj = this;
            if (task) {
                task.then(function() {
                    var ret = currentStep.callback.apply(this, arguments);
                    thisObj.currentIndex++;
                    thisObj.retryTimes = 0;
                    if (typeof ret == 'number') {
                        thisObj.step(ret);
                    } else {
                        thisObj.step(currentStep.sleep || 1000);
                    }
                }).error(function() {
                    this.retryTimes++;
                    thisObj.step(Math.min(1000 * this.retryTimes, 30000));
                });
            } else {
                thisObj.step(1);
            }
        },
        pause: function() {
            if (this.timer) {
                window.clearTimeout(this.timer);
                this.timer = null;
            }
        }
    };

    var pipline = new SearchPipline();

    function refresh() {
        console.log(currentVersion);
        pipline.reset();
        pipline.step();
    }


    pipline.add(function() {
            return searchHistogram('time', currentInterval, 'log', currentVersion.types.launch);
        },
        function(body) {
            updateChartData(DATA_TYPE_LAUNCH, body.facets[0].entries);
            return 1;
        });
    pipline.add(function() {
            if(currentVersion.types.crash) {
                return searchHistogram('time', currentInterval, 'log', currentVersion.types.crash);
            } else {
                render();
            }
        },function(body) {
            updateChartData(DATA_TYPE_CRASH, body.facets[0].entries);
            render();
            return 5000;
        });

    pipline.add(function () () {
        return searchTerms('at', 'log', currentVersion.types.crash);
    }, function(body) {

    });

    function ping() {
        client.ping({
            requestTimeout: 1000,
            hello: 'are you there?'
        }, function(error){
            if (error) {
                console.trace(error);
            }
            //window.setInterval(ping, 10000);
        });
    }

    function getVersions (callback) {
        client.indices.getMapping({index: '_all'}).then(function(body) {
            var mappings = body.log.mappings;
            var versionPattern = /([^|]+)\|([^|]+)\|([^|]+)/;
            var versions = {};
            for(var type in mappings) {
                var match = versionPattern.exec(type);
                if (match) {
                    var productNode = versions[match[2]];
                    if (!productNode) {
                        productNode = {};
                        versions[match[2]] = productNode;
                    }
                    var versionNode = productNode[match[3]];
                    if (!versionNode) {
                        versionNode = {'crash': ['crash', match[2], match[3]].join('|'),
                                       'launch': ['launch', match[2], match[3].join('|')]};
                        productNode[match[3]] = versionNode;
                    }
                    versionNode[match[1]] = type;
                }
            }
            callback(true, versions);
        }, function(error) {
            callback(false, error);
        });
    }

    ping();

    getVersions(function(successful, data) {
        if (successful) {
            fillVersions(data);
        } else {
            console.trace(data);
        }
    });
});