function getTimeInterval(granularity) {
    $.ajax({
        header: {
            'Content-Type': 'application/json;charset=utf-8'
        },
        url: 'http://localhost:8888/flow/section/get/timeInterval',
        data: {
            granularity: granularity
        },
        dataType: "json",
        type: "POST",
        success: function (data) {
            getDynamic(data)
        },
    });
}

function getFlowTime(date, granularity) {
    $.ajax({
        header: {
            'Content-Type': 'application/json;charset=utf-8'
        },
        url: 'http://localhost:8888/flow/section/get/flow_time',
        data: {
            date: date,
            granularity: granularity
        },
        dataType: "json",
        type: "POST",
        success: function (data) {
            console.log(data);
            getDynamic(data)
        },
    });
}

function getSectionFlow(lately) {
    $.ajax({
        header: {
            'Content-Type': 'application/json;charset=utf-8'
        },
        url: 'http://localhost:8888/flow/section/get/histogram',
        data: {
            lately: lately
        },
        dataType: "json",
        type: "POST",
        success: function (data) {
            setHistogram(data)
        },
    });
}

function getSectionFlowDynamicOrder(date) {
    $.ajax({
        header: {
            'Content-Type': 'application/json;charset=utf-8'
        },
        url: 'http://localhost:8888/flow/section/get/dynamic_order',
        data: {
            date: date
        },
        dataType: "json",
        type: "POST",
        success: function (data) {
            getSectionFlowSort(data)
        },
    });
}