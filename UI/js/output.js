/**
 * This page build UI rendering of logs file which user want to display on output.php page
 */
var MAX_PROCESS_COUNT = 10;

// This jquery function is for selecting/de-selecting multiple checkboxes on process filter selection panel. This panel
// is used for process selection for rendering of cpu_usage, mem_usage and res_mem_usage plt files.
$(function() {
    $(".group-checkbox:checkbox").change(function(){
        var group = this.name;
        var group_id = group.split("-")[1];
        var subgroup_id = "input[id='group[" + group_id + "]']";
        if ($(this).prop('checked')){
            $(subgroup_id).each(function (){
                $(this).prop('checked', true);
            });
        }else{
            $(subgroup_id).each(function (){
                $(this).prop('checked', false);
            });
        }
    });

});

// Submission of process selection on proper filter panel for rendering graph of selected process
$(document).ready(function() {
$(".proc_filter_form").submit(function(e){
    e.preventDefault();
    div_id = this.div_id.value;
    title = this.title.value;
    checkbox_array = "input[name='proc_list" + div_id + "[]']:checked";
    var proc_count = $(checkbox_array).length;
    if (proc_count > MAX_PROCESS_COUNT){
        alert("Maximum " + MAX_PROCESS_COUNT + " process can be selected");
        return false;
    }else if (proc_count < 1){
        alert("Select atleast 1 process");
        return false;
    }else{
	$.ajax({
            type: 'post',
            url: 'output.php',
            data: $(this).serialize(),
            success: function (data) {
		var graph_div = "#graph-" + div_id;
                $(graph_div).empty();
       	        try {
	            var response = JSON.parse(data);
                    buildOutputPageGraphView(response['graph_data_json'], response['column_json'], response['metric_data'], response['x_value'], response['xs_json'], title, div_id, graph_div);
	        } catch (e) {
		    buildGraphErrorView(data, div_id, "Graph-Error", '2', graph_div);
	      }
            }
        });
    }
});
});

// Build UI for side by side text compare view
function buildTextCompareView(json_data,test_list,div_id){
    if (div_id === "output-table-display"){
        div_class="output-text-panel";
    }else{
        div_class="compare-text-div";
    }

    var file_data_arr = JSON.parse(json_data);
    var test_arr = test_list.split(",");
    var div_size = 12 / test_arr.length;

    var main_div = $("div#" + div_id);

    for (var i=0;i<test_arr.length;i++){
        var text_div = $('<div></div>').addClass('col-sm-' + div_size  + ' ' + div_class  + ' panel panel-default');
        var text_div_heading = $('<div></div>').addClass('panel-heading')
            .attr("style","text-align:center;")
            .text(test_arr[i]);
        var text_div_body = $('<div></div>').addClass('panel-body')
        var pre_text = $('<pre></pre>').addClass('pre_text').html(file_data_arr[test_arr[i]]);
        $(text_div_body).append(pre_text);
        $(text_div).append(text_div_heading);
        $(text_div).append(text_div_body);
        $(main_div).append(text_div);
    }
}

// Build UI for graph view on output page for rendering plt file
function buildOutputPageGraphView(json_data, col_name, metric_json, x_value , xs_json , title, graphid, div_id) {
    var panel = $("<div></div>").addClass("panel panel-info partition-1");
    var pHeading = $("<div></div>").addClass("panel-heading collapse-heading");
    var pTitle = $("<h4></h4>").addClass("panel-title text-center");
    var pLink = $("<a></a>").attr('data-toggle', 'collapse').attr('href', '#graph-div-' + graphid).attr('class','collapse-link')
        .text(title);
    $(pTitle).append(pLink);
    $(pHeading).append(pTitle);
    var pGraphDiv = $("<div></div>").addClass("graph-panel panel-collapse collapse in").attr("id", 'graph-div-' + graphid);
    var pGraph = $("<div></div>").addClass("c3-graph-panel")
        .attr("id", "c3item" + graphid);
    var pFooter = $("<div></div>").addClass("metric-footer");
    var pLegend = $("<table></table>").addClass("table table-condensed sortable")
        .attr("id", "c3footer" + graphid)
        .append($("<thead></thead>")
            .append($("<tr></tr>")
                .append($("<th></th>").text("Test"))
                .append($("<th></th>").text("Min"))
                .append($("<th></th>").text("Max"))
                .append($("<th></th>").text("Average"))));
    $(pFooter).append(pLegend);
    $(pGraphDiv).append(pGraph).append(pFooter);
    $(panel).append(pHeading).append(pGraphDiv);
    if (div_id){
        $(div_id).append(panel);    
    }else{
        $("#output-panel").append(panel);    
    }
    buildGraph(json_data, col_name, metric_json, x_value ,xs_json ,title, graphid);
}

// Display error message in case of some error
function unsupportedView(message,div_id) {
    var label = $("<h2></h2").addClass("err-msg-label")
        .text(message);
    div_id = "#" + div_id;
    $(div_id).append(label);
}

// It count # of tests user is trying to compare, for desktop we allow maximum 4 test while on mobile we allow
// maximum 2 test comparison
function checkTestCount(){
    var testid = $('#testid').val();
    var compids = $('#compids').val().split(",");
    var testcount = compids.length;
    if(compids.indexOf(testid) != -1){
        alert("Compare field contains Test ID - Please remove Test ID");
        return false;
    }

    var width = $( window ).width();
    if (width < 768){
        if (testcount > 1){
            alert("On this device only 2 test comparison is allowed");
            return false;
        }
    }else{
        if (testcount > 3){
            alert("Maximum 4 test can be compared - Please remove some test from comparison.");
            return false;
        }
    }
    return true;

}

// Display tabular view for any CSV file and display it side by side
function buildFileToTableView(json_data,test_list,div_id) {
    if (div_id === "output-table-display"){
        div_class="output-text-panel";
    }else{
        div_class="compare-text-div";
    }

    var file_data_arr = JSON.parse(json_data);
    var test_arr = test_list.split(",");
    var div_size = 12 / test_arr.length;
    var main_div = $("div#" + div_id);
    for (var i=0;i<test_arr.length;i++){
        var retdata = file_data_arr[test_arr[i]];
        var lines = retdata.split("\n");
        if(lines.length < 1) {
            unsupportedView("Empty file",div_id);
            return;
        }
        var csv_div = $('<div></div>').addClass('col-sm-' + div_size  + ' ' + div_class  + ' panel panel-default');
        var csv_div_heading = $('<div></div>').addClass('panel-heading')
            .attr("style","text-align:center;")
            .text(test_arr[i]);
        var csv_div_body = $('<div></div>').addClass('panel-body').attr("id","csv_table_view");
        var table = $("<table></table>").addClass("table table-hover")
            .attr("id","csv-table");
        var thead = $("<thead></thead>");
        var thead_tr = $("<tr></tr>");
        var headers = lines[0].split(",");
        for(var j = 0; j < headers.length; ++j) {
            $(thead_tr).append($("<th></th>")
                .text(headers[j].replace(/\"/g, "")));
        }
        var tbody = $("<tbody></tbody>");
        for(var j = 1; j < lines.length; ++j) {
            if(lines[j].trim().length > 1) {
                var tbody_tr = $("<tr></tr>");
                var rowData = lines[j].split(",");
                for(var k = 0; k < rowData.length; ++k) {
                    $(tbody_tr).append($("<td></td>").text(rowData[k]));
                }
                $(tbody).append(tbody_tr);
            }
        }
        $(thead).append(thead_tr);
        $(table).append(thead);
        $(table).append(tbody);
        $(csv_div_body).append(table);
        $(csv_div).append(csv_div_heading);
        $(csv_div).append(csv_div_body);
        $(main_div).append(csv_div);
    }
}

// Display tabular view of any JSON data and display it side by side
function buildJsonToTableView(json_data,div_id) {
    var compare_data = JSON.parse(json_data);
    if (compare_data.length < 2){
        unsupportedView("Empty file",div_id);
        return;
    }

    var table = $("<table></table>").addClass("table table-hover")
        .attr("id","result-table");
    var thead = $("<thead></thead>");
    var thead_tr = $("<tr></tr>");
    var headers = compare_data["Test ID"];
    $(thead_tr).append($("<th></th>").addClass("active").text("Test ID".replace(/\"/g,"")));
    for(var j = 0; j < headers.length; ++j) {
        $(thead_tr).append($("<th></th>").addClass("test-data-td")
            .text(headers[j].replace(/\"/g, "")));
    }
    var tbody = $("<tbody></tbody>");
    for (var line_key in compare_data){
        if ((line_key.localeCompare("Test ID") != 0) && compare_data.hasOwnProperty(line_key)) {
            var tbody_tr = $("<tr></tr>");
            $(tbody_tr).append($("<td></td>").addClass("active").text(line_key));
            var line_value = compare_data[line_key];
            for(var k = 0; k < line_value.length; ++k) {
                $(tbody_tr).append($("<td></td>").addClass("test-data-td").text(line_value[k]));
            }
            $(tbody).append(tbody_tr);
        }
    }
    $(thead).append(thead_tr);
    $(table).append(thead);
    $(table).append(tbody);
    $("div#" + div_id).append(table);

}

// Switch file display based on what user have selected
function switchFileViewerFormat(referer) {
    var fileType = $(referer).find("option:selected").text().toLowerCase();
    location.search = location.search.replace(/&format=[^&$]*/i, '&format=' + fileType);
}
// Ajax call for submitting request for downloading particular log file
function downloadFIle(filename,s_compids_str){
    url = "/downloadfile.php?filename=" + filename + "&testids=" + s_compids_str;
    var http = new XMLHttpRequest();
    http.open('HEAD', url, false);
    http.send();
    if (http.status == 404){
        alert("File not found or invalid URL");
    }
    else
        window.location.href = url;
}
