# This file proccess the raw top output and docker output file to create various plt files which user can render on
# UI for analysis

import os
import datetime
import csv
import re


class ProcessOutputFiles:
    def __init__(self, logctx):
        self.lctx = logctx

    def process_output_files(self, path):
        # Process raw top output file
        self.process_top_output(path)
        # Process raw docker output file
        self.process_docker_output(path)
        return "Files processing completed"

    def process_top_output(self, path):
        """
        This function process raw top output from the file top_output.txt into three PLT files. It create seperate
        PLT file for capturing below information :
        1. cpu_usage.plt - percentage of CPU used by all the processes
        2. memory_usage.plt - percentage of RAM used by all the processes
        3. res_memory_usage.plt - physical memory used by all the processes

        """
        output_file = path + "top_output.txt"
        self.lctx.debug("Top output processing started for file : " + output_file)

        cpu_map = {}
        mem_map = {}
        res_mem_map = {}
        cpu_array = []
        mem_array = []
        res_mem_array = []
        cpu_array.append(["Time"])
        mem_array.append(["Time"])
        res_mem_array.append(["Time"])
        timestamp = 0

        if not os.path.exists(output_file):
            return "File Not Found Exception"

        cpu_file = open(path + "cpu_usage.plt", 'w+')
        if not cpu_file:
            return "Not able to create cpu_usage.plt"

        mem_file = open(path + "memory_usage.plt", 'w+')
        if not mem_file:
            return "Not able to create memory_usage.plt"

        res_mem_file = open(path + "res_memory_usage.plt", 'w+')
        if not res_mem_file:
            return "Not able to create res_memory_usage.plt"

        process_data = True
        with open(output_file) as f:
            # Reading output from top output file line by line until end of the file
            for line in f:
                line = line.strip('\n')
                line = line.strip()
                line = re.sub(' +', ' ', line)
                line = line.replace(' ', ',')

                # Skip output lines until we get new data values which starts after row starting with PID
                if line.startswith("top"):
                    process_data = False
                    continue

                # Continue processing new values for new timestamp
                if line.startswith("PID"):
                    process_data = True
                    continue

                if process_data:
                    if len(line) > 0:
                        try:
                            # If this line contain timestamp then dump the stat values for old timestamp from
                            # already populated map to respective array, this is sort of demarcation point of values
                            # for old timestamp and new timestamp values
                            datetime.datetime.strptime(line, "%Y-%m-%dT%H:%M:%S%fz")
                            cpu_array = self.update_array_from_map(timestamp, cpu_array, cpu_map)
                            mem_array = self.update_array_from_map(timestamp, mem_array, mem_map)
                            res_mem_array = self.update_array_from_map(timestamp, res_mem_array, res_mem_map)
                            timestamp = line
                            cpu_map.clear()
                            mem_map.clear()
                            res_mem_map.clear()
                        except:
                            # Continue dumping stat values for particular process in a map
                            line_array = line.split(",")
                            cpu_map[line_array[11] + " - " + line_array[0]] = line_array[8]
                            mem_map[line_array[11] + " - " + line_array[0]] = line_array[9]
                            res_mem_map[line_array[11] + " - " + line_array[0]] = line_array[5]

        # Dump data from last timestamp
        cpu_array = self.update_array_from_map(timestamp, cpu_array, cpu_map)
        mem_array = self.update_array_from_map(timestamp, mem_array, mem_map)
        res_mem_array = self.update_array_from_map(timestamp, res_mem_array, res_mem_map)

        # creating plt files by wrriting array values in a file
        self.create_plt_from_array(cpu_file, cpu_array)
        self.create_plt_from_array(mem_file, mem_array)
        self.create_plt_from_array(res_mem_file, res_mem_array)

    def process_docker_output(self, path):
        """
        This function process raw docker output from the file docker_stat.txt into two PLT files. It create seperate
        PLT file for capturing below information :
        1. cpu_usage.plt - percentage of CPU used by all docker conatiners
        2. memory_usage.plt - percentage of RAM used by all docker containers

        """
        output_file = path + "docker_stat.txt"
        self.lctx.debug("Docker stat output processing started for file : " + output_file)

        cpu_map = {}
        mem_map = {}
        
        cpu_array = []
        mem_array = []
        
        cpu_array.append(["Time"])
        mem_array.append(["Time"])
        
        timestamp = 0

        if not os.path.exists(output_file):
            return "File Not Found Exception"

        cpu_file = open(path + "docker_cpu.plt", 'w+')
        if not cpu_file:
            return "Not able to create docker_cpu.plt"

        mem_file = open(path + "docker_memory.plt", 'w+')
        if not mem_file:
            return "Not able to create docker_memory.plt"

        with open(output_file) as f:
            for line in f:
                line = line.strip('\n')
                line = line.strip()
                line = re.sub(' +', ' ', line)
                line = line.replace(' ', ',')

                # Skip output linw which starts with "NAME"
                if line.startswith("NAME"):
                    continue

                if len(line) > 0:
                    try:
                        # If this line contain timestamp then dump the stat values for old timestamp from
                        # already populated map to respective array, this is sort of demarcation point of values
                        # for old timestamp and new timestamp values
                        datetime.datetime.strptime(line, "%Y-%m-%dT%H:%M:%S%fz")
                        cpu_array = self.update_array_from_map(timestamp, cpu_array, cpu_map)
                        mem_array = self.update_array_from_map(timestamp, mem_array, mem_map)
                        
                        timestamp = line
                        cpu_map.clear()
                        mem_map.clear()
                    except:
                        # Continue dumping stat values for particular docker conatiner ID in a map
                        line_array = line.split(",")
                        cpu_map[line_array[0] + " - " + line_array[1]] = line_array[2]
                        mem_map[line_array[0] + " - " + line_array[1]] = line_array[3]

        # Dump data from last timestamp, if any
        cpu_array = self.update_array_from_map(timestamp, cpu_array, cpu_map)
        mem_array = self.update_array_from_map(timestamp, mem_array, mem_map)

        # creating plt files by wrriting array values in a file
        self.create_plt_from_array(cpu_file, cpu_array)
        self.create_plt_from_array(mem_file, mem_array)

    def create_plt_from_array(self, fh, array):
        """
        CSV writer for dumping array into CSV file. PLT files are basically CSV files which user can render on UI for
        analysis

        """
        with fh as f:
            writer = csv.writer(f)
            writer.writerows(array)
        fh.close()

    def update_array_from_map(self, ts, input_array, input_map):
        """
        This method update global array with stat values for given timestamp ts. It initialize array and then start
        appending row which capture timestamp and stat values for all the columns for that timestamp

        """
        row_count = len(input_array)
        col_count = len(input_array[0])
        if len(input_map) == 0 and ts != 0:
            # If Map is empty but ts is not 0, then we assume that all the stat value were 0.0, so we create a row for
            # this ts and put 0.0 for all column values
            temp_list = list()
            temp_list.append(ts)
            for i in range(1, col_count):
                temp_list.append(0.0)

            input_array.append(temp_list)

        elif len(input_map) > 0 and ts != 0:
            # If Map contains stat values for a set of columns and ts is not zero, then we iterate on existing column
            # value already present in array if it is present in new map then we enter new value in this new array row
            # and pops it out from map else we enter value as 0.0 for this ts
            temp_list = list()
            temp_list.append(ts)
            for i in range(1, col_count):
                if input_array[0][i] in input_map:
                    temp_list.append(input_map.get(input_array[0][i]))
                    input_map.pop(input_array[0][i], None)
                else:
                    temp_list.append(0.0)

            input_array.append(temp_list)
            row_count += 1

            if len(input_map) > 0:
                # If map contains any new column value then update array with this new column, enter 0.0 for all
                # previous ts and then put new value at the end
                for x in input_map:
                    input_array[0].append(x)
                    col_count += 1
                    for i in range(1, row_count):
                        if input_array[i][0] == ts:
                            input_array[i].append(input_map[x])
                        else:
                            input_array[i].append(0.0)

        return input_array
