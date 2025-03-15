# CSE517-FinalProj

This code is based on [AgentBench v0.1](https://github.com/THUDM/AgentBench/tree/v0.1) and [Middleware GitHub repository](https://github.com/OSU-NLP-Group/Middleware/tree/main)

## Environment Setup

1. **Conda environment:**

   ```
   conda env create -f setup.yaml
   ```

   This will create a Conda environment with all the dependencies specified in `setup.yaml`.

2. **Activating the environment:**

   ```
   conda activate <environment_name>
   ```
   (Replace `<environment_name>` with the name of your newly created Conda environment.)

## Data Setup

1. **Download the dataset:**
   
   Refer to the [Middleware GitHub repository](https://github.com/OSU-NLP-Group/Middleware/tree/main) for instructions on downloading the necessary datasets. Place the downloaded data in the `data` folder in this project.

2. **Set up the SQLite server (for BIRD benchmark):**

   Follow the instructions provided in the Middleware repository to spin up a SQLite database for the BIRD benchmark.

3. **Set up the Freebase server (for KGQA benchmark):**

   You will need access to a running Freebase server for the KGQA benchmark. Instructions can be found in the Middleware repository or in the official Freebase documentation.

## Configuration

1. **API keys:**

   Insert your API keys in the appropriate configuration files under `configs/agents/api_agents/`. Make sure any sensitive keys are kept private and **not** committed to version control.

## Usage

1. **Execute BIRD benchmark:**

   ```
   ./execute_bird.sh
   ```

2. **Execute KG benchmark:**

   ```
   ./execute_kg.sh
   ```

These scripts assume that:
- You have activated the Conda environment.
- You have set up the required databases and data folders as described above.
- You have populated the correct API keys in `configs/agents/api_agents/`.
