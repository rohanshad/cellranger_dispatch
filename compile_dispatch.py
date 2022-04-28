'''
Shorter script to read in paths from csv file and count + generate downstream folder structure
'''

import argparse as ap
import subprocess
from shutil import move, copyfile, copytree
import os 
import pandas as pd
import tarfile
from datetime import date
import time
import glob
import bcolors
import sys
import textwrap

class RNA_Pipeline_Run:
	def __init__(self, root, sample_csv_file, genome, debug):

		self.root_dir = root
		self.sample_csv_file = os.path.join(os.getcwd(),sample_csv_file)
		self.genome = os.path.join('$OAK/ref_genomes', genome)

		# Create these folders at later stage
		self.startup_generator = os.path.join(self.root_dir, 'startup_generator')
		self.mkfastq_dir = os.path.join('mkfastq_dir')


	def build_slurm_scripts(self, samples_csv_file):
		'''
		Builds slurm scripts for all samples in a for loop
		'''

		sample_df = pd.read_csv(samples_csv_file)
		print(f'Number of available samples: {bcolors.BLUE}{len(sample_df)}{bcolors.ENDC}')

		for row in range(len(sample_df)):
			sample = sample_df['patient'][row]
			segment = sample_df['segment'][row]
			fastq_path = sample_df['path'][row]

			os.makedirs(os.path.join(root_dir, 'startup_generator', sample, segment), exist_ok=True)

			slurm_header = textwrap.dedent('''\
			#!/bin/bash
			#SBATCH --job-name={sample}_{segment} 
			#SBATCH --output={sample}_{segment}.log
			#SBATCH --mail-user=rshad@stanford.edu
			#SBATCH --mail-type=end

			# Time limits 
			#SBATCH -t 24:00:00

			# Partition info
			#SBATCH --partition=owners,willhies
			#SBATCH --qos=normal
			#SBATCH --nodes=1
			#SBATCH --mem=72GB
			#SBATCH --ntasks-per-node=12

			ulimit -u 10000
			'''.format(sample=sample, segment=segment))

			run_command = self.run_cellranger_count(sample, segment, self.genome, fastq_path)

			file_path = os.path.join(self.root_dir, 'startup_generator', sample, segment, 'submit.sh')
			with open(file_path, 'w') as file:
				# Creates new file to prevent double overwrites
				file.write(slurm_header)
			with open(file_path, 'a') as file:
				# Appends run command to newly created file
				file.write(run_command)

		print(f'{bcolors.OK}Successfully generated runfiles!{bcolors.ENDC}')
		print(f'Run files generated for {len(sample_df["patient"].unique())} unique patients')
		
	def run_cellranger_count(self, sample, segment, genome, fastq_path):
		'''
		Builds command for cellranger count + output directory delivery
		'''
		formatting_bs = textwrap.dedent(f'''\
		#Creates working directory within high speed L_SCRATCH + runs everything in it
		mkdir $L_SCRATCH/workdir
		cd $L_SCRATCH/workdir

		echo "Running in L_SCRATCH"	
		''')
		
		cellranger_cmd = 'cellranger count --id={sample}_{segment}' \
				' --transcriptome={genome}' \
				' --fastqs={fastqs}' \
				' --sample={original_sample_name}' \
				' --expect-cells=10000'.format(
					sample = sample,
					segment = segment,
					genome = self.genome,
					fastqs = fastq_path,
					original_sample_name = fastq_path.rsplit('/', 1)[1]
					)
		copy_outputs = textwrap.dedent(f'''\
			
			
			#L_SCRATCH wil purge everything from memory as soon as job ends, this step copies everything back to GROUP_SCRATCH

			echo "Copying back to GROUP_SCRATCH"

			mkdir -p $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}/{sample}/{segment}
			cp -r $L_SCRATCH/workdir/{sample}_{segment}/* $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}/{sample}/{segment}

			echo "Job copied, waiting 10s to terminate..."

			sleep 10
			''')

		run_command = formatting_bs + cellranger_cmd + copy_outputs	
		return run_command

	def master_run(self):
		submit_files = glob.glob(os.path.join(self.root_dir, 'startup_generator','*', '*','*.sh'))
		for i in submit_files:
			print(f'Job for {(i.split("startup_generator/"))[1].split("/submit.sh")[0]}:')
			subprocess.run('sbatch {file}'.format(file=i), shell=True)

	def prep_root_directory(self):
		start_time = time.time()
		os.makedirs(self.root_dir, exist_ok=True)

	def run(self):
		'''
		Initializes pipeline sequentially
		'''
		print('------------------------------------')
		print(f'PREPARING ROOT DIRECTORY ON OAK')
		print('------------------------------------')
		self.prep_root_directory()
		print('------------------------------------')
		print(f'BUILDING RUNFILES')
		print('------------------------------------')
		self.build_slurm_scripts(self.sample_csv_file)
		print('------------------------------------')
		print(f'DISPATCHING CELLRANGER JOBS')
		print('------------------------------------')
		self.master_run()


if __name__ == '__main__':
	parser = ap.ArgumentParser(
		description="Compiles together runs from different dates and sources and produces single localized counts directory. \
			Sequentially will generate folder structure for a successful cellranger run in the given {--root_dir}. \
			Raw reads are first copied over from {--input_dir} following which cellranger mkfastq and cellranger count \
			(either RNAseq or ATACseq) will be distributed across muiltiple nodes. The sample ids are pulled from a user defined \
			csv file {--sample_csv}. The outputs will appear in $GROUP_SCRATCH/RNA_seq_outputs with a datestamp",

		epilog="Version 1.0; Created by Rohan Shad, MD"
	)

	parser.add_argument('-r', '--root_dir', metavar='', required=True, help='Full path to root data directory on OAK')
	parser.add_argument('-s', '--sample_csv', metavar='', required=True, help='Name of samples.csv file in the correct format (save this in root_dir)')
	parser.add_argument('-g', '--genome', metavar='', required=True, help='Name of the genome eg: refdata-cellranger-mm10-3.0.0')
	parser.add_argument('-d', '--debug', metavar='', required=False, default=False)

	args = vars(parser.parse_args())
	print(args)

	root_dir = args['root_dir']
	sample_csv = args['sample_csv']
	genome = args['genome']
	debug = args['debug']

	print(f'Using genome: {bcolors.BLUE}{genome}{bcolors.END}')

	rna_pipe = RNA_Pipeline_Run(root_dir, sample_csv, genome, debug)

	start_time = time.time()
	
	### START RUN JOB ### 
	rna_pipe.run()


	print(f'{bcolors.OK}Jobs submitted on slurm{bcolors.END}. Type in "squeue -u your_sunet_here" to check for status')
	print(f'Output files will be delivered to $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}')
	print(f'Elapsed time: {round((time.time() - start_time), 2)}s')

