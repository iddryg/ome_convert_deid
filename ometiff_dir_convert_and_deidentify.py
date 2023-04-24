# Ian Dryg
# Started April 13, 2023
# version: 20230424
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

parser.add_argument('--rgb', action='store_true')

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


# this should work for qptiffs, svs, and scn files
file_types = ['.qptiff', '.svs', '.scn']

# assemble the list of files to convert
print('')
# get files
files_to_convert = []
for filetype in file_types:
    temp_files = glob(input_dir + '/*' + filetype)
    files_to_convert = files_to_convert + temp_files

print('Files to convert: ')
print(files_to_convert)
print('')

# get directories names for the intermediate raw files by removing '.qptiff' from the file names
# split the filename into a list of strings where '.' was the separator (because we want to get rid of '.qptiff'
# take all elements in that list of strings except for the last one (which will be '.qptiff'
# join them back together
orig_raw_dirs = [''.join(file.split('.')[:-1]) for file in files_to_convert]
# put in their own directory not with the input files
raw_dirs = [output_dir + '/' + os.path.split(path)[1] for path in orig_raw_dirs]
print('Intermediate raw directories: ')
print(raw_dirs)
print('')


# bioformats2raw
# this suppresses the output text (which was an insane amount of text)
with io.capture_output() as captured:
    for file,raw_dir in zip(files_to_convert,raw_dirs):
        # --series=0 will only take the first series and drop the rest.
        # The first series is the tiled pyramid image. The other series are thumbnail, 
        # etc other stuff we don't really need or want. 
        #!bioformats2raw {file} {raw_dir} --series=0
        print(' >>> bioformats2raw ' + str(file) + ' ' + str(raw_dir) + ' --series=0')
        subprocess.check_call(['bioformats2raw', str(file), str(raw_dir), '--series=0'])


# get list of output files...
out_files = [output_dir + '/' + os.path.split(path)[1] + '.ome.tiff'  for path in orig_raw_dirs]


# raw2ometiff
for raw_dir,outfile in zip(raw_dirs, out_files):
        #!raw2ometiff {raw_dir} {outfile}
        if args.rgb:
            print(' >>> raw2ometiff --rgb ' + str(raw_dir) + ' ' + str(outfile))
            subprocess.check_call(['raw2ometiff', '--rgb', str(raw_dir), str(outfile)])
        else:
            print(' >>> raw2ometiff ' + str(raw_dir) + ' ' + str(outfile))
            subprocess.check_call(['raw2ometiff', str(raw_dir), str(outfile)])


# function to gather metadata into a dict
def get_img_metadata(ome1,filename):
    img_meta_dict = {
        'Component':'ImagingLevel2',
        'Filename':filename,
        'File Format':'OME-TIFF',
        'HTAN Participant ID':'',
        'HTAN Parent Biospecimen ID':'',
        'HTAN Data File ID':'',
        'Channel Metadata Filename':'',
        'Imaging Assay Type':'H&E',
        'Protocol Link':'',
        'Software and Version':'',
        'Microscope':ome1.instruments[0].microscope.model,
        'Objective':ome1.instruments[0].objectives[0].model,
        'NominalMagnification':ome1.instruments[0].objectives[0].nominal_magnification,
        'LensNA':'',
        'WorkingDistance':'',
        'WorkingDistanceUnit':'',
        'Immersion':'',
        'Pyramid':'',
        'Zstack':'',
        'Tseries':'',
        'Passed QC':'',
        'Comment':'',
        'FOV number':'',
        'FOVX':'',
        'FOVXUnit':'',
        'FOVY':'',
        'FOVYUnit':'',
        'Frame Averaging':'',
        'Image ID':ome1.images[0].id,
        'DimensionOrder':ome1.images[0].pixels.dimension_order,
        'PhysicalSizeX':ome1.images[0].pixels.physical_size_x,
        'PhysicalSizeXUnit':'',
        'PhysicalSizeY':ome1.images[0].pixels.physical_size_y,
        'PhysicalSizeYUnit':'',
        'PhysicalSizeZ':ome1.images[0].pixels.physical_size_z,
        'PhysicalSizeZUnit':'',
        'Pixels BigEndian':ome1.images[0].pixels.big_endian,
        'PlaneCount':len(ome1.images[0].pixels.planes),
        'SizeC':ome1.images[0].pixels.size_c,
        'SizeT':ome1.images[0].pixels.size_t,
        'SizeX':ome1.images[0].pixels.size_x,
        'SizeY':ome1.images[0].pixels.size_y,
        'SizeZ':ome1.images[0].pixels.size_z,
        'PixelType':'',
    }
    return img_meta_dict


# use ome_types to interact with ome-tiff xml metadata
# then overwrite the old metadata with the new metadata using tifffile.tiffcomment()
# initialize results list for metadata logging
res_list = []
for file in out_files:
    
    # get ome.tiff filename
    filename = os.path.split(img)[1]
    
    # import metadata from tiff
    ome = ot.from_tiff(file)
    print('ome.tiff before metadata stripping: ')
    print(ome)
    
    # get image metadata
    meta_dict = get_img_metadata(ome1,filename)
    # convert dict to dataframe
    res_df = pd.DataFrame.from_dict([meta_dict])
    # append res_df to results list
    res_list.append(res_df)
    
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


# concatenate all the results dataframes together
final_res_df = pd.concat(res_list)

# replace Nones with empty strings
final_res_df.fillna("",inplace=True)

# Save results dataframe as excel file
final_res_df.to_excel(output_dir + '/htan_ome_metadata.xlsx')  
