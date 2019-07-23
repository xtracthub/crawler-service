import os
import zipfile
import tarfile
from subprocess import run

""" This file takes a file path and decompresses it to an extraction path. 
    compressions = ["zip*", "gz*", "tar*", "z*", "zipx", "zz", "tgz" "rar", "7z", "s7z"]
    @Author: Tyler J. Skluzacek (skluzacek@uchicago.edu)
    @LastEdited: 08/20/2017 
"""


def unzip(file_path, extract_path):
    """Extracts contents of a .zip file to another directory.

    Parameters:
    file_path (str): File path to .zip file.
    extract_path (str): File path to extract contents of file_path to.
    """
    if file_path.endswith(".zip"):
        zipfile.ZipFile(file_path, 'r').extractall(extract_path)


def untar(file_path, extract_path):
    """Extracts contents of a .tar.gz, .tar, or .tgz file to another directory.

    Creates a directory with the file's name and extracts contents into it.

    Parameters:
    file_path (str): File path to .tar.gz, .tar, or .tgz file.
    extract_path (str): File path to extract contents of file_path to.
    """
    if file_path.endswith(".tar.gz") or file_path.endswith(".tar") or file_path.endswith(".tgz"):
        base_path = os.path.basename(file_path)
        base_path_no_extention = base_path[:base_path.index('.')]
        extract_path = os.path.join(extract_path, base_path_no_extention)
        tarfile.TarFile.open(file_path).extractall(extract_path)


def unZ(file_path, extract_path):
    """Extracts contents of a .z or .gz file to another directory.

    Extracts using a unix command and moves the extracted file to extract_path.

    Parameters:
    file_path (str): File path to .z  or .gz file.
    extract_path (str): File path to extract contents of file_path to.
    """
    if file_path.endswith(".gz") or file_path.endswith(".Z"):
        run("gzip -dk " + file_path, shell=True)

        file_name_no_extension = os.path.join(os.path.dirname(file_path),
                                              os.path.splitext(os.path.basename(file_path))[0])
        run("mv " + file_name_no_extension + " " + extract_path, shell=True)


def decompress_file(file_path, extract_path):
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
        print(".Z or .gzip error")
        pass


# 7z Test code
# def un7z(file_path, extract_path):
#     with libarchive.file_reader(file_path) as e:
#         for entry in e:
#             # if os.path.basename(str(entry)) is '':
#             #     os.mkdir(os.path.join(extract_path, str(entry)))
#
#             with open(extract_path + os.path.basename(str(
#                     entry)), 'wb') as f:
#                 for block in entry.get_blocks():
#                     f.write(block)
