<?php
/**
 * This is output page for log files rendering. It support 3 display types like Graph for PLT files, tabular view for
 * CSV files and Plain text view for all other files. It also allows comparison across multiple test for a
 * particular file.
 */

require('lib/auth.php');
include 'process_data.php';

/**
 * Ajax call handler for graph rendering of top output files, as we restrict rendering of maximum 10 processes on
 * graph, we take process list as input from user for rendering graph
 */

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $file = getParam('file', 'POST');
    $div_id = getParam('div_id', 'POST');
    $proc_list = getParam('proc_list' . $div_id, 'POST');
    $response_json = buildGraphDataForFilteredColumns($file, $proc_list);
    echo $response_json;
    return;
}

$allTestData = array();
$testId = getParam('testid');
$filename = getParam('filename');
$outputFormat = getParam('format');
$extension = pathinfo($filename, PATHINFO_EXTENSION);

/**
 * Input validations
 */
if (!$testId) {
    diePrint("No test ID passed in", "Error");
}
if (!is_numeric($testId)) {
    diePrint("testid is not valid.", "Error");
}

if (!$outputFormat) {
    $outputFormat = "plain";
}

$origTestData = getTestById($db, $testId, true);
if (!$origTestData) {
    diePrint("Could not find test ID: $testId", "Error");
}


// Fetching framework details
$frameworkId = $origTestData['frameworkid'];
$frameworkData = getFrameworkById($db, $frameworkId, true);
$frameworkName = $origTestData['frameworkname'];
$allTestData[] = $origTestData;

$compIds = getParam('compids');
$compIds_orig = $compIds;
if (!strcmp($extension, "log")) {
    $compIds = '';
}

if ($compIds && !preg_match('/^\d+(,\d+)*$/', $compIds)) {
    diePrint("compids is not valid.", "Error");
}

// Fetching all test details (main test and comparison tests)
if ($compIds) {
    foreach (explode(',', $compIds) as $compId) {
        $testData = getTestById($db, $compId, true);
        if (!$testData) {
            diePrint("Could not find compare test ID: $compId", "Error");
        }
        if ($testData['frameworkname'] != $frameworkName) {
            diePrint("Cannot compare test IDs from different frameworks.", "Error");
        }
        $allTestData[] = $testData;
    }
}

$s_compids_str = "";
$s_compids = array();
if (!empty($compIds)) {
    $s_compids = explode(",", getParam("compids"));
    $s_compids_str = getParam("compids");
}
$s_testid = "none";
if (isset($_GET["testid"])) {
    $s_testid = getParam("testid");
    array_unshift($s_compids, $s_testid);
    if (!empty($s_compids_str)) {
        $s_compids_str = $s_testid . "," . $s_compids_str;
    } else {
        $s_compids_str = $s_testid;
    }
}

function formatFilePath($path, $mapping)
{
    $pattern = '/%(STAT|EXEC|RESERVED){1}HOST,([0-9]+)%(.*)/';
    if (preg_match($pattern, $path, $output_array)) {
        switch ($output_array[1]) {
            case "STAT":
                return $mapping["statistics"][$output_array[2]] . $output_array[3];
                break;
            case "EXEC":
                return $mapping["execution"][$output_array[2]] . $output_array[3];
                break;
            case "RESERVED":
                return $mapping["reserved"][$output_array[2]] . $output_array[3];
                break;
        }
    } else {
        return $mapping["execution"][0] . "/" . $path;
    }
}

$pageTitle = "Test Report ($testId)";

$s_compids_arr = explode(",", $s_compids_str);
$bind_vale_str = "";
for ($x = 1; $x <= sizeof($s_compids_arr); $x++) {
    $bind_vale_str = $bind_vale_str . ":test" . $x;
    if ($x !== sizeof($s_compids_arr)) {
        $bind_vale_str = $bind_vale_str . ",";
    }
}

try {
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $db->beginTransaction();
    $query = "SELECT frameworkname,TestInputData.title,TestInputData.purpose,priority,timeout,cc_list,
                  TestInputData.creation_time,modified,start_time,end_time FROM TestInputData
                  JOIN ApplicationFrameworkMetadata USING(frameworkid) WHERE testid=:testid";
    $stmt = $db->prepare($query);
    $stmt->bindValue(':testid', $s_testid, PDO::PARAM_INT);
    $stmt->execute();
    $test_info_data = $stmt->fetch(PDO::FETCH_ASSOC);

    $query = "SELECT testid,hostassociationid,hostname,name FROM HostAssociation JOIN HostAssociationType
                   USING(hostassociationtypeid) WHERE testid IN (" . $bind_vale_str . ") ORDER BY hostassociationid";
    $stmt = $db->prepare($query);
    for ($x = 1; $x <= sizeof($s_compids_arr); $x++) {
        $stmt->bindValue(':test' . $x, $s_compids_arr[$x - 1], PDO::PARAM_INT);
    }

    $stmt->execute();
    $test_hosts_result = $stmt->fetchAll(PDO::FETCH_ASSOC);

    foreach ($test_hosts_result as $row) {
        if (!isset($test_info_data[$row["testid"]][$row["name"]])) {
            $test_info_data[$row["testid"]][$row["name"]] = array();
        }
        array_push($test_info_data[$row["testid"]][$row["name"]], $row["hostname"]);
    }
} catch (PDOException $e) {
    $db->rollBack();
    returnError("MySQL error: " . $e->getMessage());
}
$err_threshold = sizeof($s_compids);
$marked = 0;
$full_paths = "";
foreach ($s_compids as $l_id) {
    if ($marked) {
        $full_paths .= ",";
    }
    $marked = 1;
    $report_path = "test_data/" . $test_info_data["frameworkname"] . "/$l_id/results/" .
        formatFilePath($filename, $test_info_data[$l_id]);
    $report_path_file = strpos($report_path, ":") ? substr($report_path, 0, strpos($report_path, ":")) : $report_path;
    if (!file_exists($report_path_file)) {
        $err_threshold--;
    }
    $full_paths .= $report_path;
}
include_once('lib/header.php');

?>
<link href="css/c3.css" rel="stylesheet" type="text/css">
<script src="https://d3js.org/d3.v3.min.js" charset="utf-8"></script>
<script src="js/c3.min.js" charset="utf-8"></script>
<script src="js/bootstrap-sortable.js"></script>
<script src="js/test_report.js"></script>
<script src="js/output.js"></script>


<div class="content-wrapper" id="page-content">
    <div id="main-panel-alt-top">
        <?php
        if (!strcmp($extension, "log")) {
            echo "  <div class='col-md-9' id='action-buttons-div-left' style='display:none'>\n";
        } else {
            echo "  <div class='col-md-9' id='action-buttons-div-left'>\n";
        }
        echo "    <form class='zero-margin form-inline' role='form' onSubmit='return checkTestCount()'>\n";
        echo "      <div class='input-group' style='z-index:0'>\n";
        echo "        <input type='text' class='form-control h-30' id='compids' name='compids' value='$compIds' placeholder='Example: 100,102,105'>\n";
        echo "        <input type='hidden' name='testid' id='testid' value='$testId'>\n";
        echo "        <input type='hidden' name='filename' value='$filename'>\n";
        echo "        <input type='hidden' name='format' value='$outputFormat'>\n";
        echo "        <span class='input-group-btn'>\n";
        echo "        <button type='submit' class='btn btn-primary btn-action'>\n";
        echo "          Compare\n";
        echo "        </button>\n";
        echo "       </span>\n";
        echo "      </div>\n";
        echo "    </form>\n";
        echo "  </div>\n";
        if (!strcmp($extension, "log")) {
            echo "<div class='col-md-3 col-xs-12 action-buttons-alt' id='action-buttons-div' style='display:none'>\n";
        } else {
            echo "<div class='col-md-3 col-xs-12 action-buttons-alt' id='action-buttons-div'>\n";
        }
        ?>
        <select class="form-control" onchange="switchFileViewerFormat(this)">
            <option <?php echo $outputFormat == "graph" ? "selected" : ""; ?>>Graph</option>
            <option <?php echo $outputFormat == "table" ? "selected" : ""; ?>>Table</option>
            <option <?php echo $outputFormat == "plain" ? "selected" : ""; ?>>Plain</option>
        </select>
    </div>
    <br><br>
    <div class="col-md-6 action-buttons-alt" id="action-buttons-div">
        <button type="button" onclick="downloadFIle('<?php echo $filename ?>','<?php echo $s_compids_str ?>')"
                class="btn btn-success btn-action">
            <i class="fa fa-download fa-lg" aria-hidden="true"></i>
            &nbsp;Download
        </button>
        <button type="button" onclick="collapseAllGraph()" class="btn btn-info btn-action">
            <i class="fa fa-minus-square fa-lg"></i>
            &nbsp;Collapse
        </button>
        <button type="button" onclick="expandAllGraph()" class="btn btn-info btn-action">
            <i class="fa fa-plus-square fa-lg"></i>
            &nbsp;Expand
        </button>
    </div>
</div>
<div class="col-xs-12" id="content-div">
    <div id="main-panel-alt">
        <?php
        $file_arr = explode(',', $full_paths);
        $file_arr_final = array_reverse($file_arr);
        $valid_path = validate_file_path($file_arr_final[0]);
        if ($valid_path === true) {
            $file_content = array_map("str_getcsv", file($file_arr_final[0], FILE_SKIP_EMPTY_LINES));
            $file_header = array_shift($file_content);
        }

        $act_file = pathinfo($filename, PATHINFO_BASENAME);
        if ($outputFormat == "graph") {
            /**
             * Build a graph view if output format is graph, For CSV and text files this view will throw error as we can
             * only plot graph for PLT files.
             */
            echo "<div id='output-panel'>";
            buildOutputGraphView($full_paths, $s_compids_str, $act_file);
            echo "</div>";
        } else if ($outputFormat == "table") {
            /**
             * Build a tabular view if output format is table, below are different types of rendering based on file type
             * 1. For plt files, we render file in tabular format and display each comma separated value in a table cell
             * 2. For CSV files, we render file in tabular format, for key pair CSV we join join in case of comparison
             *    or else in case of multi column CSV we just do side by side compare
             * 3. For all other format, we render them as plain text file
             */
            echo "<div id='output-panel'>";
            echo "<div class='panel panel-info'>\n";
            echo "<div class='panel-heading'>\n";
            echo "<h3 class='panel-title centered'>$act_file</h3>\n";
            echo "</div>\n";
            echo "<div class='panel-body zero-padding' id='output-table-display'></div>\n";
            echo "</div>\n";
            echo "</div>";
            $file_arr = explode(',', $full_paths);
            $file_arr = array_reverse($file_arr);
            $testid_arr = explode(',', $s_compids_str);
            if (strpos($extension, 'plt') !== false) {
                $file_data_array = buildTestCompareData($file_arr[0], $testid_arr[0]);
                if (strcmp(gettype($file_data_array), "string") !== 0) {
                    $file_data_json = json_encode($file_data_array);
                    echo "    <script>buildFileToTableView('$file_data_json', '$testid_arr[0]','output-table-display');</script>\n";
                } else {
                    echo "  <script> buildGraphErrorView('$file_data_array','output-table-display','Error','3'); </script>\n";
                }
            } else if ((strpos($extension, 'csv') !== false)) {
                $csv_col_count = getCsvColumnCount($full_paths);
                if ($csv_col_count == 2) {
                    $csv_compare = generateCompareCsv($full_paths, $s_compids_str);
                    $csv_compare_json = json_encode($csv_compare);
                    echo "    <script>buildJsonToTableView('$csv_compare_json', 'output-table-display');</script>\n";
                } else {
                    $file_data_array = buildTestCompareData($full_paths, $s_compids_str);
                    if (strcmp(gettype($file_data_array), "string") !== 0) {
                        $file_data_json = json_encode($file_data_array);
                        echo "    <script>buildFileToTableView('$file_data_json', '$s_compids_str','output-table-display');</script>\n";
                    } else {
                        echo "  <script> buildGraphErrorView('$file_data_array','output-table-display','Error','3'); </script>\n";
                    }
                }
            } else {
                $file_data_array = buildTestCompareData($full_paths, $s_compids_str);
                if (strcmp(gettype($file_data_array), "string") !== 0) {
                    $file_data_json = json_encode($file_data_array);
                    echo "    <script>buildTextCompareView('$file_data_json', '$s_compids_str','output-table-display');</script>\n";
                } else {
                    echo "  <script> buildGraphErrorView('$file_data_array','output-table-display','Error','3'); </script>\n";
                }

            }
        } else {
            /**
             * For all other format, we just render files as simple text files
             */
            echo "<div id='output-panel'>";
            echo "<div class='panel panel-info'>\n";
            echo "<div class='panel-heading'>\n";
            echo "<h3 class='panel-title centered'>$act_file</h3>\n";
            echo "</div>\n";
            echo "<div class='panel-body zero-padding' id='output-table-display'></div>\n";
            echo "</div>\n";
            echo "</div>";
            $file_data_array = buildTestCompareData($full_paths, $s_compids_str);
            if (strcmp(gettype($file_data_array), "string") !== 0) {
                $file_data_json = json_encode($file_data_array);
                echo "    <script>buildTextCompareView('$file_data_json', '$s_compids_str','output-table-display');</script>\n";
            } else {
                echo "  <script> buildGraphErrorView('$file_data_array','output-table-display','Error','3'); </script>\n";
            }
        }
        ?>
    </div>
</div>
</div> <!-- .content-wrapper -->
</main> <!-- .cd-main-content -->

<!--Graph zoom view panels -->
<div class="modal fade" id="zoomModal" role="dialog">
    <div class="modal-dialog modal-full">
        <div class="modal-content">
            <div class="modal-header modal-header-blue">
                <button type='button' class='close close-blue' data-dismiss='modal'>&times;</button>
                <h4 class='modal-title h4-blue'></h4>
            </div>
            <div class='modal-body'>
                <div class='c3-graph-panel' id='zoom-body'></div>
                <div class='metric-footer'></div>
            </div>
            <div class='modal-footer'>
                <button type='button' class='btn btn-default' data-dismiss='modal'>Close</button>
            </div>
        </div>
    </div>
</div>

<script>
    $(document).ready(function () {
        $("#zoomModal").on('shown.bs.modal', function () {
            $("#zoom-body").data("c3-chart").flush();
        });
        buildTopNavBar('<?php echo $frameworkName; ?>', '<?php echo $testId; ?>');
        setDescription('Test Information');
        buildUserAccountMenu('<?php echo $userId; ?>');
        buildLeftPanel();
        buildLeftPanelTest(<?php echo $frameworkId; ?>, '<?php echo $userId; ?>');
        buildLeftPanelViews('<?php echo $testId; ?>', '<?php echo $compIds_orig; ?>');
        buildLeftPanelFramework('<?php echo $frameworkName; ?>', <?php echo $frameworkId; ?>);
        buildLeftPanelGlobal();
        <?php
        addFrameworkDropdownJS($db, $userId);
        addTestResults("test_data/$frameworkName/$testId/results", $origTestData["execution"], $testId, $compIds_orig, $origTestData["execution_script_location"]);
        if (array_key_exists("execution", $origTestData)) {
            echo "createLabel('System Metrics')\n";
            addSystemMetrics("test_data/$frameworkName/$testId/results", $origTestData["execution"], $testId, $compIds_orig, "exec");
        }
        if (array_key_exists("statistics", $origTestData)) {
            addSystemMetrics("test_data/$frameworkName/$testId/results", $origTestData["statistics"], $testId, $compIds_orig, "stat");
        }
        $hosts['EXECHOST'] = $origTestData["execution"];
        if (array_key_exists("statistics", $origTestData)) {
            $hosts['STATHOST'] = $origTestData["statistics"];
        }
        addLogs("test_data/$frameworkName/$testId/results", $hosts, $testId, $compIds_orig);
        ?>
        loadNavigationBar();
        flushAllCharts();
    });
</script>

<?php include_once('lib/footer.php'); ?>

