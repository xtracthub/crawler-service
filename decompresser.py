import os, zipfile, tarfile, zlib, gzip, pathlib
from subprocess import run

""" This file takes a list of compressed files, extracts contents, and disposes of compressed file. 
    compressions = ["zip*", "gz*", "tar*", "z*", "zipx", "zz", "tgz" "rar", "7z", "s7z"]
    @Author: Tyler J. Skluzacek (skluzacek@uchicago.edu)
    @LastEdited: 08/20/2017 
"""


def unzip(file_path, extract_path):
    if not(file_path.endswith(".zip")):  # check for ".zip" extension
        file_name = os.path.abspath(file_path)
        zipfile.ZipFile(file_name, 'r').extractall(extract_path)


def untar(file_path, extract_path):
    if file_path.endswith(".tar.gz") or file_path.endswith(".tar"):
        file_name = os.path.abspath(file_path)
        base_path = os.path.basename(file_name)
        base_path_no_extention = base_path[:base_path.index('.')]
        extract_path = os.path.join(extract_path, base_path_no_extention)

        tarfile.TarFile.open(file_name).extractall(extract_path)


# Waiting for a .Z file sample
def unZ(file_path):
    if file_path.endswith(".Z"):  # check for ".zip" extension
        file_name = os.path.abspath(file_path)  # get full path of files
        run("uncompress " + file_path, shell=True)  # Decompress


#def ungz(file_path, extract_path):




def list_files(file_path, extract_path):
    try:
        unzip(file_path, extract_path)

    except:
        print(".zip error")
        pass

    try:
        untar(file_path, extract_path)
    except:
        print(".tar or .tar.gz error")
        pass

    try:
        unZ(file_path, extract_path)
    except:
        print(".Z error")
        pass


# with zipfile.ZipFile('C:/Users/space/Documents/CS/CDAC/official_xtract'
#                      '/xtract-crawler/decompresser_tests/Zip/junk.zip',
#                      'r') as zip_ref:
#     zip_ref.extractall('.')
unZ('C:/Users/space/Documents/CS/CDAC/official_xtract/xtract-crawler'
    '/decompresser_tests/SEP94L.Z')

