import glob, os
import pickle
from project_dir import project_dir

# жирновато, у меня в память не влезает
if __name__ == '__main__':
    index_file_dir = project_dir
    os.chdir(index_file_dir)
    index = []
    for filename in glob.glob("index*.pkl"):
        with open(index_file_dir + filename, 'rb') as file:
            index.append(pickle.load(file))

    print(index)
