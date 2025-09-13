# buzzline-03-data-git-hub

Streaming data does not have to be simple text.
Many of us are familiar with streaming video content and audio (e.g. music) files.

Streaming data can be structured (e.g. csv files) or
semi-structured (e.g. json data).

We'll work with two different types of data, and so we'll use two different Kafka topic names.
See [.env](.env).

## First, Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first.
**Python 3.11 is required.**

## Second, Copy This Example Project & Rename

1. Once the tools are installed, copy/fork this project into your GitHub account
   and create your own version of this project to run and experiment with.
2. Name it `buzzline-03-yourname` where yourname is something unique to you.

Additional information about our standard professional Python project workflow is available at
<https://github.com/denisecase/pro-analytics-01>.

Use your README.md to record your workflow and commands.

---

## Task 0. If Windows, Start WSL

- Be sure you have completed the installation of WSL as shown in [https://github.com/denisecase/pro-analytics-01/blob/main/01-machine-setup/03c-windows-install-python-git-vscode.md](https://github.com/denisecase/pro-analytics-01/blob/main/01-machine-setup/03c-windows-install-python-git-vscode.md).

- We can keep working in **Windows VS Code** for editing, Git, and GitHub.
- When you need to run Kafka or Python commands, just open a **WSL terminal** from VS Code.

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window. 

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground.
5. Keep this terminal open - Kafka will run here
6. Watch for "started (kafka.server.KafkaServer)" message

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment
2. Activate the virtual environment
3. Upgrade pip
4. Install from requirements.txt

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Start a Kafka JSON Producer

This producer generates streaming JSON data for our topic.

In VS Code, open a terminal.
Use the commands below to activate .venv (if not active), and start the producer.

Windows:

```shell
.venv\Scripts\activate
py -m producers.json_producer_case
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m producers.json_producer_case
```

What did we name the topic used with JSON data?
Hint: See the producer code and [.env](.env).

## Task 4. Start a Kafka JSON Consumer

This consumer processes streaming JSON data.

In VS Code, open a NEW terminal in your root project folder.
Use the commands below to activate .venv, and start the consumer.

Windows:

```shell
.venv\Scripts\activate
py -m consumers.json_consumer_case
```

Mac/Linux:

```zsh
source .venv/bin/activate
python3 -m consumers.json_consumer_case
```

What did we name the topic used with JSON data?
Hint: See the consumer code and [.env](.env).

---

## Task 5. Start a Kafka CSV Producer

Follow a similar process to start the csv producer.
You will need to:

1. Open a new terminal (yes another)!
2. Activate your .venv.
3. Know the command that works on your machine to execute python (e.g. py or python3).
4. Know how to use the -m (module flag to run your file as a module).
5. Know the full name of the module you want to run. Hint: Look in the producers folder.

What did we name the topic used with csv data?
Hint: See the producer code and [.env](.env).

Hint: Windows:

```shell
.venv\Scripts\activate
py -m producers.csv_producer_case
```

## Task 6. Start a Kafka CSV Consumer

Follow a similar process to start the csv consumer.
You will need to:

1. Open a new terminal (yes another)!
2. Activate your .venv.
3. Know the command that works on your machine to execute python (e.g. py or python3).
4. Know how to use the -m (module flag to run your file as a module).
5. Know the full name of the module you want to run. Hint: Look in the consumers folder.

What did we name the topic used with csv data?
Hint: See the consumer code and [.env](.env).

Hint: Windows:

```shell
.venv\Scripts\activate
py -m consumers.csv_consumer_case
```

---

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## About the Smart Smoker (CSV Example)

A food stall occurs when the internal temperature of food plateaus or
stops rising during slow cooking, typically between 150°F and 170°F.
This happens due to evaporative cooling as moisture escapes from the
surface of the food. The plateau can last for hours, requiring
adjustments like wrapping the food or raising the cooking temperature to
overcome it. Cooking should continue until the food reaches the
appropriate internal temperature for safe and proper doneness.

The producer simulates a smart food thermometer, sending a temperature
reading every 15 seconds. The consumer monitors these messages and
maintains a time window of the last 5 readings.
If the temperature varies by less than 2 degrees, the consumer alerts
the BBQ master that a stall has been detected. This time window helps
capture recent trends while filtering out minor fluctuations.

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

# P3 JSON & CSV Streaming with Kafka + Polars (Eevee Evolutions Edition)

Real-time streaming pipelines using Apache Kafka, Python 3.11, and Polars.
You’ll run two flavors side-by-side:

JSON pipeline (optional)

CSV pipeline with Polars analytics + a console ASCII bar chart (Top-5)

Works on Windows (with WSL for Kafka) and macOS/Linux.

## Install & Setup

Windows (with WSL for Kafka)

   1. Open VS Code - open this project folder.
   2. Powershell terminal - create and activate venv:

```shell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
# If you get an execution policy error:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
   3. Install dependences (in the venv):
```shell
python -m pip install --upgrade pip wheel setuptools
python -m pip install -r requirements.txt
```
   4. WSL (Ubuntu) for Kafka only
   . In VS Cose: Terminal - New Terminal:
```shell
wsl
```

macOS/Linux
   1. Terminal in the project root:
```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install -r requirements.txt
```
Note: If `python3.11` not found, install it (macOS: `brew install python@3.11` ; Ubuntu `subo apt install python 3.11 python 3.11-venv`)

## Start Kafka

Windows: run Kafka inside WSL

macOS/Linux: run Kafka directly in your shell.

```bash
# Make helper script executable (from project root, in WSL/macOS/Linux)
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh

# Start Kafka (KRaft)
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```
Note: Keep this terminal open (Kafka runs in the foreground).
      Wait until you see “started (kafka.server.KafkaServer)”.

## Run the Pipelines


## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.
