### Clarification

This folder shows a dummy pipeline of our feature generation process. 
There are many differences between this lightweight pipeline and our deployment code:
- Our deployment code is based on spark, kafka, and a customized data processing framework that we are unable to share. 
- We build this lightweight pipeline primarily using pandas within a relatively short period, and we haven't tested on months of data. So there might be some bugs that we are unaware of. 
- Our original deployment (feature generation and active learning) is running on a daily bases. Features generated on Day N depends on Day N-1's data and learning feedbacks. This interative updating process is not demonstrated in this notebook.   

### [Important] Neo4j GraphDB
----
- We leverage neo4j graphDB to maintain topological structures of our network. 

#### Neo4j GraphDB Installation and Configuration
- Before we start to compute topological features, we need to install and configure graph database. The following code was deployed on neo4j graph database community version 4.4.0
- Offical installation guidance can be found at: https://debian.neo4j.com/?_ga=2.50993706.2051114848.1695324818-590444767.1695324818

- To install neo4j 4.4.0:
    - `wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -`
    - `echo 'deb https://debian.neo4j.com stable 4.4' | sudo tee /etc/apt/sources.list.d/neo4j.list`
    - `sudo apt-get update`
    - `sudo apt-get install neo4j=1:4.4.0`
- After installation complete, check the installation status in the command line: 
    - `neo4j status`
    - if you see something like `neo4j is not running`, then we have a successful installation.
- Next we need to start the graph db:
    - `sudo neo4j start`
- After we start the neo4j database, you need to change the default password of neo4j. Type command: 
    - `sudo cypher-shell`
    - If you see `connection refused`, wait another 20 seconds and retry. There is a short delay after starting the graph db.
    - The default username is:`neo4j`, and the default password is:`neo4j`. Change the password to your own password.
    - If you are using our test server, the default password is set to `acsac`.
- Leave the `cypher-shell` using `Ctrl + D`
- **Important.** We need to config the database configuration before we start the database. There are two approaches:
    - **First approach**: 
        - you can copy our provided configuration to rewrite the configurate.
        - `sudo mv /etc/neo4j/neo4j.conf /etc/neo4j/neo4j_bk.conf`
        - `sudo cp /home/ubuntu/bcndetection/dummypipeline/neo4j.conf /etc/neo4j/neo4j.conf`
    - **Second approach**: 
        - you can directly edit the file by navigating to the default config file at `/etc/neo4j/neo4j.conf`. 
        - You need **sudo** to edit this file, e.g. `sudo vim /etc/neo4j/neo4j.conf`
        - **Change:** `dbms.directories.import=$path_to_repo$/dummypipeline/dummydata/graphtmp`
        - For example: `dbms.directories.import=/home/ubuntu/bcndetection/dummypipeline/dummydata/graphtmp`
        - Next, we need to **add** the following `dbms.security.procedures.unrestricted=apoc.*` in the neo4j.conf
        - **Save the configuration file.**
- **Important.** Install Neo4j APOC Core functions:
    - `sudo mv /var/lib/neo4j/labs/apoc-*core.jar /var/lib/neo4j/plugins/`
- **Important.** Configure Neo4j APOC function:
    - copy the provided configuration file to the same neo4j configuration folder:
    - `sudo cp /home/ubuntu/bcndetection/dummypipeline/apoc.conf /etc/neo4j/apoc.conf`
- **Important.** We need to restart the database after the configuration with command: `sudo neo4j restart`
- By default, you can visualize the graphdb at: `http://localhost:7474/browser/`

#### Demos
Just like the periodicity detection demos, users can engage with the demo notebooks by initiating JupyterLab:

```
jupyter-lab
```
As the graph-based features rely on some historical data, please run `0_genfeats_nongraph.ipynb` at first. 


### Folder Structure
    .
    ├── dummydata                    # dummy data
        ├── features                 # features directory
        ├── graphtmp                 # tmp folder for graph db
        ├── *.csv/parquet            # historical/periodicity/other data
    ├── src                          # Source files
    ├── 0_genfeats_nongraph.ipynb    # demo for non-graph features (run this FIRST)
    ├── 1_genfeats_graph.ipynb       # demo for graph features
    ├── omit_demo.ipynb         # demo for omitting noise simulation and visualization
    ├── playwithsigsimulation.ipynb     # visualizing periodic signals with various noise configurations.
    ├── plotting.ipynb         # plotting script for figure 4.
    ├── requirements.txt       # pip package dependencies
    ├── conda-lock.yml         # conda-lock file
    ├── bcndetection.tar.gz    # compressed everything for anonymous reviewing process
    └── README.md
