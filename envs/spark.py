import os, time
import torch
import logging
import pandas as pd

import envs.params as p
from statistics import mean

class SparkEnv:
    def __init__(
        self,
        csv_path: str = p.SPARK_CONF_INFO_CSV_PATH,
        config_path: str = p.CONF_PATH,
        workload: str = None,
        workload_size: str = None,
        alter: bool = True,
        debugging: bool = False
    ):
        self.config_path=config_path
        
        csv_data = pd.read_csv(csv_path, index_col=0)

        self.dict_data = csv_data.to_dict(orient='index')
        
        self.workload = workload if workload is not None else 'join'
        
        self.debugging = debugging
        
        self.alter = False if self.debugging else alter
        
        self.workload_size = workload_size
        
        self.timeout = 1000 if workload_size in ["tiny", "small", "large"] else 2000
        
        self.start_dataproc()
        
        if self.alter:
            self._alter_hibench_configuration(self.workload_size)
        self.fail_conf_flag = False
        
    def _alter_hibench_configuration(self, workload_size=None):
        if workload_size is None:
            workload_size = 'large' # self.workloads_size[self.workload]
            
        HIBENCH_CONF_PATH = os.path.join(p.DATA_FOLDER_PATH, f'{workload_size}_hibench.conf')
        logging.info("Altering hibench workload scale..")
        logging.info(f"Workload ***{self.workload}*** with ***{workload_size}*** size..")
        os.system(f'scp {HIBENCH_CONF_PATH} {p.MASTER_ADDRESS}:{p.MASTER_CONF_PATH}/hibench.conf')


    def apply_configuration(self, config_path=None):
        if self.debugging:
            logging.info("DEBUGGING MODE, skipping to apply the given configuration")
        else:
            self._apply_configuration(config_path)
            
    def run_configuration(self, load:bool):
        if self.debugging:
            start = time.time()
            logging.info(f"DEBUGGING MODE, skipping to benchmark the given configuration --> ### LOAD? {load}")
            end = time.time()
            logging.info(f"DEBUGGING MODE, [HiBench]⏱ data loading takes {end - start} s ⏱")
        else:
            self._run_configuration(load)
            
    def get_results(self):
        if self.debugging:
            logging.info("DEBUGGING MODE, getting results from the local report file..")
            
            f = open(p.HIBENCH_REPORT_PATH, 'r')
            report = f.readlines()
            f.close()
            
            from random import sample
            rand_idx = sample(range(1, len(report)),1)[0]
            
            duration = report[rand_idx].split()[-3]
            tps = report[rand_idx].split()[-2]
            logging.info(f"DEBUGGING MODE, the recorded results are.. Duration: {duration} s Throughput: {tps} bytes/s")
            return float(duration)
        else:
            return self._get_results()
    
    def _apply_configuration(self, config_path=None):
        config_path = self.config_path if config_path is None else config_path
        
        logging.info("Applying created configuration to the remote Spark server.. 💨💨")
        os.system(f'scp {config_path} {p.MASTER_ADDRESS}:{p.MASTER_CONF_PATH}/add-spark.conf')
        
    def _run_configuration(self, load:bool):
        """
            TODO:
            !!A function to Save configuration should be implemented on other files!!
        """
        
        if load:
            start = time.time()
            os.system(f'timeout {self.timeout} ssh {p.MASTER_ADDRESS} "bash --noprofile --norc -c scripts/prepare_wk/load_{self.workload}.sh"')
            end = time.time()
            logging.info(f"[HiBench] data loading (seconds) takes {end - start}")

        exit_code = os.system(f'timeout {self.timeout} ssh {p.MASTER_ADDRESS} "bash --noprofile --norc -c scripts/run_wk/run_{self.workload}.sh"')

        if exit_code > 0:
            logging.warning("💀Failed benchmarking!!")
            logging.warning("UNVALID CONFIGURATION!!")
            self.fail_conf_flag = True
        else:
            logging.info("🎉Successfully finished benchmarking")
            self.fail_conf_flag = False
                        
    def _get_results(self) -> float:
        logging.info("Getting result files..")
        if self.fail_conf_flag:
            duration = 10000
            tps = 0.1
        else:
            os.system(f'ssh {p.MASTER_ADDRESS} "bash --noprofile --norc -c scripts/report_transport.sh"')
            f = open(p.HIBENCH_REPORT_PATH, 'r')
            report = f.readlines()
            f.close()
            
            duration = report[-1].split()[-3]
            tps = report[-1].split()[-2]
        logging.info(f"The recorded results are.. Duration: {duration} s Throughput: {tps} bytes/s")

        return float(duration)
    
    # Clear hdfs storages in the remote Spark nodes
    def clear_spark_storage(self):
        if self.debugging:
            logging.info("[Google Cloud Platform|Dataproc] 🛑 Skipping cleaning Spark storage!!")
        else:
            exit_code = os.system(f'ssh {p.MASTER_ADDRESS} "bash --noprofile --norc -c scripts/clear_hibench.sh"')
            if exit_code > 0:
                logging.warning("💀Failed cleaning Spark Storage!!")
            else:
                logging.info("🎉Successfully cleaning Spark Storage")
    
    def start_dataproc(self):
        if self.debugging:
            logging.info("[Google Cloud Platform|Dataproc] 🛑 Skipping start Spark instances")
        else:
            logging.info("[Google Cloud Platform|Dataproc] 🔥 Start Spark instances")
            os.system(p.GCP_DATAPROC_START_COMMAND)
        
    def stop_dataproc(self):
        if self.debugging:
            logging.info("[Google Cloud Platform|Dataproc] 🛑 Skipping stop Spark instances")
        else:
            logging.info("[Google Cloud Platform|Dataproc] ⛔ Stop Spark instances")
            os.system(p.GCP_DATAPROC_STOP_COMMAND)