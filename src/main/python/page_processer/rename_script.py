#! /usr/bin/python3

import json
import os
from project_dir import project_dir
import datetime

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('start time: {}'.format(start_time))

    input_path = project_dir + 'crawled'
    output_path = project_dir + 'crawled_renamed'
    files = os.listdir(input_path)
    filesMap = dict()
    total = len(files)
    processed = 0
    for file in files:
        with open("{}/{}".format(input_path, file), 'r') as f:
            lines = f.readlines()
            link = lines[0]
            newName = abs(hash(link))
            filesMap[newName] = link
            with open("{}/{}.txt".format(output_path, newName), 'w+') as of:
                of.writelines(lines[1:])

            processed += 1
            if processed % 100 == 0:
                print("processed: {} / {}".format(processed, total))

    print('saving file..')
    with open(project_dir + "indexFilesMap.txt", 'w+') as of:
        json.dump(filesMap, of)

    end_time = datetime.datetime.now()
    print('time elapsed: {}'.format(end_time - start_time))
