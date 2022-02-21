# Cellranger Dispatch
A somewhat fully contained "Fire and forget" python wrapper for single cell RNA pre-processing with cellranger.

***

#### Installation and Setup

Tested on Ubuntu 20.02 and CentOS7 
These steps need to be done once. 

1. If working on Sherlock the conda environments are already setup

	```
	source activate rnaseq
	```
2. Install dependencies with pip (skip if on hiesinger sherlock partition)

	```
	pip install -r requirements.txt
	```
3. Ensure cellranger works and you can access it from the terminal. Add these two lines to your bashrc file

	```
	vim ~/.bashrc
	```
	Hit 'i' on the keyboard and add these two lines somewhere:

	```
	export PATH="$GROUP_HOME/single_cell/cellranger-6.1.2:$PATH"
	export PATH="$GROUP_HOME/single_cell/cellranger-atac-1.2.0:$PATH"
	```

***

#### Running cellranger_dispatch

The program copies over data from the genome sequencing service center (Usually a folder within `"/oak/stanford/projects/genomics/labs/mfischbe"`) into our own `$OAK` or `$GROUP_SCRATCH` directory of choice. There after passing some checks and reading in the provided samples.csv file, the script will build FASTQ files using all available CPU cores. A separate batch submit script is generated for each sample, and dispatched to the SLURM job scheduler to run either atac or RNAseq alignment. All count matrices and job outputs are delivered to a folder on `$GROUP_SCRATCH` to a folder called `RNA_seq_outputs_{DD_MM_YY}`. 


The program takes requires the following arguments to run (```python cellranger_dispatch.py --help``` to see more)

```
  -i , --input_source   Name of raw data dump folder on /oak/stanford/projects/genomics/labs/mfischbe
  -r , --root_dir       Full path to new data directory on $OAK or $GROUP_SCRATCH
  -s , --sample_csv     Name of samples.csv file in the correct format
  -g , --genome         Name of the genome eg: refdata-cellranger-mm10-3.0.0
  -t , --task           set as either "rna" or "atac"
  -d , --debug 			
```

#### Example usage:

1. The script should be run within a sherlock compute node with adequate CPU and RAM for a time duration that covers the generation of FASTQ files. If you try running ```cellranger_dispatch.py``` within a login node it will error out.

	```
	salloc -p willhies -c 12 --mem 96GB -t 12:00:00 -J rohans_legacy
	```

2. Load the conda environment in the compute node

	```
	source activate rnaseq
	```

3. Run the program with appropriate inputs

	```
	python cellranger_dispatch.py -i 220127_A00509_0436_AH5CHKDMXY -r '/scratch/groups/willhies/testing/2_19_2022' -s mouse_samples.csv -t rna -g refdata-cellranger-mm10-3.0.0
	```

The program can resume from where it was interrupted, whether while copying data over to a new directory, or while building FASTQ files. Re-run the command and let it do it's work. This should work 95% of the time straight out of the box with minimal tinkering. If it doesn't then debugging will be fun. 


#### Debugging:

1. Where is the data? 
	Check the way you've named the folders is wrong or it points to the wrong location. The program prints out what it has parsed from your inputs, if those folder names look wrong then you know what to fix.

2. csv formatting issues
	If your csv file is incorrectly formatted or not present in the same location as where you ran the `cellranger_dispatch.py` script from, it won't work and will break. The script will thankfully tell you if it has any problems with the csv file. 

3. Rsync inode / IOerrors / AllocationErrors:
	Seen sometimes when moving data within `$OAK`, if this happens contant sherlock support and ask for help and forgiveness. 

4. Runs too slow? 
	Each sample for cellranger count / cellranger-atac count is setup to run across 12 cores and 72GB RAM. You can bump this up manually by changing the hardcoded defaults. 


