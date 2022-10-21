'''
Master script to generate folder structure, build fastqs, and run cellranger pipeline
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
	def __init__(self, input_dir, root, sample_csv_file, build_fastqs, task, genome, debug):

		#self.input_dir = os.path.join('/oak/stanford/projects/genomics/labs/mfischbe', input_dir)
		#self.root_dir = os.path.join('/oak/stanford/groups/willhies/rna_seq_rawdata', root)
		self.input_dir = input_dir
		self.root_dir = root
		self.sample_csv_file = sample_csv_file
		self.build_fastqs = build_fastqs
		self.task = task
		self.genome = os.path.join('$OAK/ref_genomes', genome)

		# Create these folders at later stage
		self.startup_generator = os.path.join(self.root_dir, 'startup_generator')
		self.mkfastq_dir = os.path.join('mkfastq_dir')

	def run_cellranger_atac_count(self, sample, genome):
		'''
		Builds command for cellranger-atact count + output directory delivery
		'''
		formatting_bs = textwrap.dedent(f'''\
		#Creates working directory within $GROUP_SCRATCH + runs everything in it
		mkdir $GROUP_SCRATCH/workdir
		cd $GROUP_SCRATCH/workdir

		echo "Running in GROUP_SCRATCH"	
		''')
		
		cellranger_cmd = 'cellranger-atac count --id={sample}' \
				' --reference={genome}' \
				' --fastqs={fastqs}' \
				' --sample={sample}'.format(
					sample = sample,
					genome = self.genome,
					fastqs = os.path.join(self.root_dir,self.mkfastq_dir,'outs','fastq_path')
					)
		copy_outputs = textwrap.dedent(f'''\
			
			echo "Moving to GROUP_SCRATCH"
			mkdir $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}/
			mv $GROUP_SCRATCH/workdir/{sample} $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}/

			echo "Job copied, waiting 10s to terminate..."

			sleep 10
			''')
			
		run_command = formatting_bs + cellranger_cmd + copy_outputs	
		return run_command

	def run_cellranger_count(self, sample, genome):
		'''
		Builds command for cellranger count + output directory delivery
		'''
		formatting_bs = textwrap.dedent(f'''\
		#Creates working directory within high speed L_SCRATCH + runs everything in it
		mkdir $L_SCRATCH/workdir
		cd $L_SCRATCH/workdir

		echo "Running in L_SCRATCH"	
		''')
		
		cellranger_cmd = 'cellranger count --id={sample}' \
				' --transcriptome={genome}' \
				' --fastqs={fastqs}' \
				' --sample={sample}' \
				' --expect-cells=10000'.format(
					sample = sample,
					genome = self.genome,
					fastqs = os.path.join(self.root_dir, self.mkfastq_dir,'outs','fastq_path')
					)
		copy_outputs = textwrap.dedent(f'''\
			
			
			#L_SCRATCH wil purge everything from memory as soon as job ends, this step copies everything back to GROUP_SCRATCH

			echo "Copying back to GROUP_SCRATCH"

			mkdir $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}/
			cp -r $L_SCRATCH/workdir/{sample} $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}/

			echo "Job copied, waiting 10s to terminate..."

			sleep 10
			''')
		run_command = formatting_bs + cellranger_cmd + copy_outputs	
		return run_command

	def build_slurm_scripts(self, samples_csv_file, task):
		'''
		Builds slurm scripts for all samples in a for loop
		'''

		sample_df = pd.read_csv(samples_csv_file)
		for sample in sample_df['Sample'].unique():
			slurm_header = textwrap.dedent('''\
			#!/bin/bash
			#SBATCH --job-name={sample}
			#SBATCH --output={sample}.log
			##SBATCH --mail-user=rshad@stanford.edu
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
			'''.format(sample=sample))

			if task == 'rna':
				run_command = self.run_cellranger_count(sample, self.genome)

			elif task == 'atac':
				run_command = self.run_cellranger_atac_count(sample, self.genome)


			file_path = os.path.join(self.root_dir, 'startup_generator', sample, 'submit.sh')
			with open(file_path, 'w') as file:
				# Creates new file to prevent double overwrites
				file.write(slurm_header)
			with open(file_path, 'a') as file:
				# Appends run command to newly created file
				file.write(run_command)

		print(f'{bcolors.OK}Successfully generated runfiles!{bcolors.ENDC}')
		print(f'Run files generated for {len(sample_df["Sample"].unique())} samples')
		

	def check_inputs(self, samples_csv_file):
		print('Checking csv sample inputs for formatting errors...')
		sample_df = pd.read_csv(self.sample_csv_file)


	def master_run(self):
		#submit_files = glob.glob(os.path.join(self.root_dir, 'startup_generator','*','*.sh'))
		sample_df = pd.read_csv(self.sample_csv_file)
		for i in sample_df['Sample']:
			print(f'Job for {i}:')
			subprocess.run('sbatch {file}'.format(file=os.path.join(self.root_dir, 'startup_generator', i, 'submit.sh')), shell=True)


	def untar_files(self):
		for f in os.listdir(self.root_dir):
			if f[-3:] == 'tar':
				tar = tarfile.open(os.path.join(self.root_dir, filename))
				tar_extract_path = os.path.join(self.root_dir, filename[:-4])
				tar.extractall(tar_extract_path)
				tar.close()
				print(f'Extracted {f}')
				move(os.path.join(self.root_dir, f), self.archive)

			elif f[-6:] == 'tar.gz':
				tar = tarfile.open(os.path.join(self.root_dir, filename))
				tar_extract_path = os.path.join(self.root_dir, filename[:-7])
				tar.extractall(tar_extract_path)
				tar.close()
				print(f'Extracted {f}')
				move(os.path.join(self.root_dir, f), self.archive)
				
			else:
				pass


	def prep_root_directory(self):
		start_time = time.time()
		
		print(f'Copying data from genomics core directory {self.input_dir}')
		print(f'{bcolors.BLUE}This will take a while, grab a redbull or something...{bcolors.ENDC}')

		os.makedirs(self.root_dir, exist_ok=True)
		subprocess.call(f'ml system rclone && rclone copy {self.input_dir} {self.root_dir} -P --stats-one-line --transfers=12', shell=True)
		print(f"Failsafe rsync if rclone copy didn't work:")
		subprocess.run('rsync -azhv {src}/ {dest} --info=progress2 --info=name0'.format(src=self.input_dir, dest=self.root_dir), shell=True)

		print(f'Copying complete, elapsed time: {round((time.time() - start_time), 2)}s')
		print('Preparing working directory structure')

		os.makedirs(self.startup_generator, exist_ok=True)

	def make_fastqs(self, samples_csv_file):
		'''
		Generates FASTQ files based on the data copied over from the sequencing core and the csv file.
		'''
		sample_df = pd.read_csv(self.sample_csv_file)
		for sample in sample_df['Sample']:
			os.makedirs(os.path.join(self.root_dir, 'startup_generator', sample), exist_ok=True)

		print('Directory structure created')

		if self.build_fastqs is True:
			os.chdir(self.root_dir)
			mkfastq_command = 'cellranger mkfastq --id="{mkfastq_dir}"' \
						' --run="{main_folder}"' \
						' --csv="{sample_csv_file}"'\
						' --jobmode="local"' \
						' --rc-i2-override=True'.format(
							mkfastq_dir=self.mkfastq_dir,
							main_folder=self.root_dir,
							sample_csv_file=self.sample_csv_file
						)
			
			print(mkfastq_command)

			bcl2fastq_load = 'module load biology bcl2fastq'
			p = subprocess.Popen('{cmd1};{cmd2}'.format(cmd1=bcl2fastq_load, cmd2=mkfastq_command), shell=True)
			streamdata = p.communicate()[1]
			
			if p.returncode != 0:
				print(f'{bcolors.ERR}Fastq generation FAILED{bcolors.ENDC}')
				sys.exit()
			else:
				print(f'{bcolors.OK}Fastqs created!{bcolors.ENDC}')
		else:
			if os.path.exists(os.path.join(self.root_dir, "fastq_path")):
				print(os.path.join(self.root_dir, "fastq_path"))
				print(f'{bcolors.OK}Fastqs available!{bcolors.ENDC}')
				
				subprocess.call(f'ml system rclone && rclone copy {os.path.join(self.root_dir, "fastq_path")} {os.path.join(self.root_dir, self.mkfastq_dir, "outs", "fastq_path")} -P --stats-one-line --transfers=12', shell=True)
				print(f"Failsafe rsync if rclone copy didn't work:")
				subprocess.call(f'rsync -azhv {os.path.join(self.root_dir, "fastq_path")} {os.path.join(self.root_dir, self.mkfastq_dir, "outs")} --info=progress2 --info=name0', shell=True)
			else:
				print(f'{bcolors.OK}No fastqs avaialble in {os.path.join(root_dir,"fastq_path")},{bcolors.ENDC}')


	def run(self):
		'''
		Initializes pipeline sequentially
		'''
		print('------------------------------------')
		print(f'PREPARING ROOT DIRECTORY ON OAK')
		print('------------------------------------')
		self.prep_root_directory()
		self.untar_files()
		print('------------------------------------')
		print(f'BUILDING FASTQ FILES')
		print('------------------------------------')
		self.make_fastqs(self.sample_csv_file)
		print('------------------------------------')
		print(f'BUILDING RUNFILES')
		print('------------------------------------')
		self.build_slurm_scripts(self.sample_csv_file, self.task)
		print('------------------------------------')
		print(f'DISPATCHING CELLRANGER JOBS')
		print('------------------------------------')
		self.master_run()



if __name__ == '__main__':
	parser = ap.ArgumentParser(
		description="Automates a bunch of stuff I used to do for Fischbein Lab. \
			Sequentially will generate folder structure for a successful cellranger run in the given {--root_dir}. \
			Raw reads are first copied over from {--input_dir} following which cellranger mkfastq and cellranger count \
			(either RNAseq or ATACseq) will be distributed across muiltiple nodes. The sampel ids are pulled from a user defined \
			csv file {--sample_csv}. The outputs will appear in $GROUP_SCRATCH/RNA_seq_outputs with a datestamp",

		epilog="Version 1.0; Created by Rohan Shad, MD"
	)

	parser.add_argument('-i', '--input_source', metavar='', required=False, help='Name of raw data dump folder if avaialble, eg: /oak/stanford/projects/genomics/labs/mfischbe')
	parser.add_argument('-r', '--root_dir', metavar='', required=True, help='Full path to root data directory on OAK')
	parser.add_argument('-s', '--sample_csv', metavar='', required=True, help='Name of samples.csv file in the correct format (save this in root_dir)')
	parser.add_argument('-f', '--build_fastqs', metavar='', required=True, default=True, help='By default assumes no fastqs are built')
	parser.add_argument('-g', '--genome', metavar='', required=True, help='Name of the genome eg: refdata-cellranger-mm10-3.0.0')
	parser.add_argument('-t', '--task', metavar='', required=True, help='set as either "rna" or "atac"')
	parser.add_argument('-d', '--debug', metavar='', required=False, default=False)

	args = vars(parser.parse_args())
	print(args)

	
	input_dir = args['input_source']
	root_dir = args['root_dir']
	sample_csv = args['sample_csv']
	genome = args['genome']
	task = args['task']
	debug = args['debug']
	build_fastqs = args['build_fastqs']

	print(f'Using genome: {bcolors.BLUE}{genome}{bcolors.END}')
	print(f'Processing experiment: {bcolors.BLUE}{task}{bcolors.END}')

	rna_pipe = RNA_Pipeline_Run(input_dir, root_dir, sample_csv, build_fastqs, task, genome, debug)

	start_time = time.time()
	
	### START RUN JOB ### 
	rna_pipe.run()


	print(f'{bcolors.OK}Jobs submitted on slurm{bcolors.END}. Type in "squeue -u your_sunet_here" to check for status')
	print(f'Output files will be delivered to $GROUP_SCRATCH/RNA_seq_outputs_{date.today().strftime("%m_%d_%y")}')
	print(f'Elapsed time: {round((time.time() - start_time), 2)}s')





