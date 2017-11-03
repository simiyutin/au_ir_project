import json
import os

if __name__ == '__main__':
    input_path = '../../../../crawled_part_1k_75k'
    files = os.listdir(input_path)
    filesMap = dict()
    for file in files:
        with open("{}/{}".format(input_path, file), 'r') as f:
            lines = f.readlines()
            link = lines[0]
            newName = abs(hash(link))
            filesMap[newName] = link
            with open("../../../../crawled_processed_for_index/{}.txt".format(newName), 'w+') as of:
                of.writelines(lines[1:])

    with open("../../../../indexFilesMap.txt", 'w+') as of:
        json.dump(filesMap, of)