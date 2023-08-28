### ReadME
-----
This repo contains the code implementation of our proposed periodicity detection algorithm in paper.

We provide three demo jupyter-notebooks `gauss/insert/omit_demo.ipynb` to generate the results in Section 4.2 Algorithm Evaluation Figure 4. 

We anticipate the reproduced results using these three notebooks to be highly similar to the original results. 

**Note** that due to the randomization process in both signal simulation and periodicity detection process, the detection results may slightly deviate from the original figure, but the overall pattern and results should be consistent. 

#### Testing Environment
------
We've tested this repo on our server (note that this server is not the machine deployed in the paper):

```
OS: Ubuntu 22.04.2 LTS
CPU: Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz
Thread(s) per core: 2
Core(s) per socket: 4
Socket(s): 1
Memory: 32GB
```

We chose jupyter-notebooks for creating demos due to their high level of interactivity, and suggest that reviewers install [JupyterLab](https://jupyter.org/install) in order to effectively run and engage with these notebooks.

The required dependencies are specified in `requirements.txt`, and we advise users to utilize a `conda` environment to prevent any potential package conflicts

#### Install required packages with conda environment
----
If you're unfamiliar with `conda`, please kindly refer to the official installation guide for [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html). Alternatively, you can opt for the installation of [Miniconda](https://docs.conda.io/en/latest/miniconda.html), which is recommended if you're looking for a minimal installation of conda environment.

Once you have the conda environment installed, proceed to the git repository directory and execute the following subsequent commands to install the required dependencies.

```
cd bcndetection
conda create --name bcn-env python=3.8.3
conda activate bcn-env
pip install -r requirements.txt
```
Executing these commands will install Python version 3.8.3 along with all the essential packages that we have tested our code with. Subsequently, users can engage with the demo notebooks by initiating JupyterLab:

```
jupyter-lab
```


### Folder Structure
    .
    ├── data                    # pregenerated simulated signals
        ├── gauss               # pregenerated signals with gaussian noise 
        ├── omt                 # pregenerated signals with omitting noise
        ├── insert              # pregenerated signals with insertion noise
    ├── results                 # results from previous runs
    ├── src                     # Source files
        ├── robustperiod        # fork of the implementations of RobustPeriod (https://github.com/ariaghora/robust-period)
        ├── sigsimulation.py    # signal simulation code
        ├── *.py                # our implementation of BAYWATCH, UPNSCA, STATS-based, and our proposed algorithm
    ├── gauss_demo.ipynb        # demo for shifting noise simulation and visualization
    ├── insert_demo.ipynb       # demo for insertion noise simulation and visualization
    ├── omit_demo.ipynb         # demo for omiting noise simulation and visualization
    ├── playwithsigsimulation.ipynb     # visualizing periodic signals with various noise configurations.
    ├── plotting.ipynb         # plotting script for figure 4.
    └── README.md
 