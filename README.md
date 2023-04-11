Overview
========

Welcome to Astronomer! This repository hosts an Astro project featuring various example DAGs that demonstrate diverse methods for transferring data between tasks. 

To utilize this repo, first [install the Astro CLI](https://docs.astronomer.io/astro/cli/install-cli), and then execute astro dev start within the local clone of this repository to initiate an Airflow instance.

Please note that some of the DAGs require additional configuration to function correctly, specifically the taskflow_kpo_example and the traditional_kpo_example. These examples necessitate access to a Kubernetes cluster and, in the case of the latter, a Docker image must be supplied for execution.