# Ian Dryg
# Started April 13, 2023
# version: 20230418
# this script converts an image file (qptiff) to ome.tiff, and removes metadata containing identification like dates. 
# receives input arguments: 
# --input_dir: input directory containing qptiffs to convert
# --output_dir: output directory for converted ome.tiffs

# If no input is given, will just use the current working directory. 

# example:
# python ometiff_dir_convert_and_deidentify.py --input_dir 2023_04_qptiff_testing --output_dir 2023_04_qptiff_testing_out

# import libraries
import pandas as pd
import numpy as np
import argparse
import ome_types as ot
from ome_types import to_xml
from ome_types.model.simple_types import UnitsLength
from ome_types.model.channel import AcquisitionMode, Channel, ChannelID
import tifffile
from glob import glob
from IPython.utils import io
from tiffinspector import TiffInspector
import xml.etree.ElementTree as ET
import subprocess
import os

# Receive input from the call
parser = argparse.ArgumentParser(
    description = 'Removes some metadata from tiff files'
)

parser.add_argument('--input_dir',
                    type=str,
                    help='the folder containing qptiffs to convert')

parser.add_argument('--output_dir',
                    type=str,
                    help='the folder to put the output ome.tiffs')

args = parser.parse_args()

print('')
# Figure out the input and output directories based on the argument parser...
if args.input_dir is not None:
    input_dir = args.input_dir
    print("input directory for qptiffs: %s" % input_dir)
else:
    input_dir = os.getcwd()
    print("using current working directory for inputs: " + input_dir)


if args.output_dir is not None:
    output_dir = args.output_dir
    
    # If folder doesn't exist, create it
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    
    print("output directory for converted files: %s" % output_dir)
else:
    output_dir = os.getcwd()
    print("using current working directory for outputs: " + output_dir)


# assemble the list of qptiffs to convert
print('')
# get files
qptiff_files = glob(input_dir + '/*.qptiff')
print('Files to convert: ')
print(qptiff_files)
print('')

# get directories names for the intermediate raw files by removing '.qptiff' from the file names
# split the filename into a list of strings where '.' was the separator (because we want to get rid of '.qptiff'
# take all elements in that list of strings except for the last one (which will be '.qptiff'
# join them back together
qptiff_raw_dirs = [''.join(file.split('.')[:-1]) for file in qptiff_files]
# put in their own directory not with the input files
raw_dirs = [output_dir + '/' + os.path.split(path)[1] for path in qptiff_raw_dirs]
print('Intermediate raw directories: ')
print(raw_dirs)
print('')


# bioformats2raw
# this suppresses the output text (which was an insane amount of text)
with io.capture_output() as captured:
    for file,raw_dir in zip(qptiff_files,raw_dirs):
        # --series=0 will only take the first series and drop the rest.
        # The first series is the tiled pyramid image. The other series are thumbnail, 
        # etc other stuff we don't really need or want. 
        #!bioformats2raw {file} {raw_dir} --series=0
        subprocess.check_call(['bioformats2raw', str(file), str(raw_dir), '--series=0'])


# get list of output files...
out_files = [output_dir + '/' + os.path.split(path)[1] + '.ome.tiff'  for path in qptiff_raw_dirs]


# raw2ometiff
for raw_dir,outfile in zip(raw_dirs, out_files):
        #!raw2ometiff {raw_dir} {outfile}
        subprocess.check_call(['raw2ometiff', '--rgb', str(raw_dir), str(outfile)])


# use ome_types to interact with ome-tiff xml metadata
# then overwrite the old metadata with the new metadata using tifffile.tiffcomment()
for file in out_files:
    
    # import metadata from tiff
    ome = ot.from_tiff(file)
    print('ome.tiff before metadata stripping: ')
    print(ome)
    
    # remove image acquisition date from each image
    for img_num in range(len(ome.images)):
        ome.images[img_num].acquisition_date = None
    
    # remove all structured annotations
    ome.structured_annotations = []
    print('ome.tiff after metadata stripping: ')
    print(ome)
    
    # update ome-tiff with new xml
    new_xml = ot.to_xml(ome)
    tifffile.tiffcomment(file, new_xml.encode())







