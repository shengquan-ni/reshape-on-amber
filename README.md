# Reshape
This is the code for the implementation of Reshape on the Amber engine. Amber is the backend engine for a service called Texera. More details about Texera and how to build it can be found at [Texera's github](https://github.com/Texera/texera).

# Building the project
```console
cd core
./scripts/build.sh
```
## Running the project:
1. Open a command line and navigate to the cloned repository. If you are on Windows, you need to use [Git Bash](https://gitforwindows.org/) as a Linux bash shell in order to run shell scripts.

2. Navigate to the `core` directory
```console
cd core
```
Then build the project. 
```console
./scripts/build.sh
```
Depending on your environment, it may take a few minutes (around 2 minutes to 6 minutes).

3. Start the Texera Web server. In the `core` directory:
```console
./scripts/server.sh
```
Wait until you see the message `org.eclipse.jetty.server.Server: Started`

4. Start the Texera worker process. Open a new terminal window. In the `core` directory:
```console
./scripts/worker.sh
```
Wait until you see the message `---------Now we have 1 nodes in the cluster---------`

Note: (if `./scripts/worker.sh` gives a "permission denied error", just do `chmod 755 scripts/worker.sh` to grant an execute permission to the file).

5. Open a browser and access `http://localhost:8080`.
