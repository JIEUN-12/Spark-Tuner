import os, time
import subprocess
import logging
import pandas as pd

import envs.params as p
from statistics import mean

class PostgresEnv:
    def __init__(
        self,
        csv_path: str = p.POSTGRES_CONF_INFO_CSV_PATH,
        config_path: str = p.CONF_PATH,
        workload: str = None,
        debugging: bool = False,
        remote_ip: str = None,
    ):
        self.config_path=config_path
        
        csv_data = pd.read_csv(csv_path, index_col=0)

        self.dict_data = csv_data.to_dict(orient='index')
        
        self.workload = workload if workload is not None else 'ycsb-a'
        
        self.debugging = debugging
               
        self.workload_size = 'none'
        
        self.timeout = 400

        self.fail_conf_flag = False
        
        self.result_logs = None
        
        self.remote_ip = remote_ip
        
        if self.remote_ip is None:
            self.remote_ip = p.POSTGRES_SERVER_ADDRESS
            self.remote_dbms_path = p.POSTGRES_SERVER_POSTGRES_PATH
            self.remote_dbms_conf_path = p.POSTGRES_SERVER_CONF_PATH
        else:
            self.remote_dbms_path = p.POSTGRES_SERVER_2_POSTGRES_PATH
            self.remote_dbms_conf_path = p.POSTGRES_SERVER_2_CONF_PATH
            self.config_path = p.CONF_TMP_PATH

    def apply_configuration(self, config_path=None):
        if self.debugging:
            logging.info("DEBUGGING MODE, skipping to apply the given configuration")
        else:
            self._apply_configuration(config_path)
            
    def run_configuration(self, load:bool):
        if self.debugging:
            logging.info(f"DEBUGGING MODE, skipping to benchmark the given configuration")
        else:
            self._run_configuration(load)
            
    def get_results(self):
        if self.debugging:
            from random import random
            tps = random() * 1000
            return float(tps)
        else:
            return self._get_results()
    
    def _apply_configuration(self, config_path=None):
        config_path = self.config_path if config_path is None else config_path
        
        logging.info("Applying created configuration to the remote PostgreSQL server.. 💨💨")
        os.system(f'sshpass -p {p.POSTGRES_SERVER_PASSWSD} scp {config_path} {self.remote_ip}:{self.remote_dbms_conf_path}/add-postgres.conf')
        self._restart_postgres()
        
    def _run_configuration(self, load:bool):       
        if self.workload == 'ycsb-a':
            run_command = f'timeout {self.timeout} sshpass -p {p.POSTGRES_SERVER_PASSWSD} ssh {self.remote_ip} {self.remote_dbms_path}/run_workloada.sh'
        elif self.workload == 'ycsb-b':
            run_command = f'timeout {self.timeout} sshpass -p {p.POSTGRES_SERVER_PASSWSD} ssh {self.remote_ip} {self.remote_dbms_path}/run_workloadb.sh'
        
        logging.info("Running benchmark..")
        result = subprocess.run(run_command, shell=True, capture_output=True, text=True)
        
        self.result_logs = result.stdout
        self.result_exit_code = result.returncode

        if self.result_exit_code > 0 or self._analyze_error(self.result_logs):
            logging.warning("💀Failed benchmarking!!")
            logging.warning("UNVALID CONFIGURATION!!")
            self.fail_conf_flag = True
        else:
            logging.info("🎉Successfully finished benchmarking")
            self.fail_conf_flag = False
                        
    def _analyze_error(self, log_lines) -> bool:
        error_lines = [s for s in log_lines.split('\n') if 'FAILED' in s or 'Return=ERROR' in s]
        
        if len(error_lines) > 0:
            return True
        else:
            return False
    
    def _get_results(self) -> float:
        if self.fail_conf_flag:
            duration = 0
            tps = 0.1
            logging.info(f"[💀 ERROR OCCURRED 💀]The recorded results are.. Duration: {duration} s Throughput: {tps} bytes/s")
        else:
            duration, tps = [s.split()[-1] for s in self.result_logs.split('\n') if '[OVERALL]' in s]
            logging.info(f"The recorded results are.. Duration: {duration} s Throughput: {tps} bytes/s")

        return float(tps)
    
    def _restart_postgres(self):
        logging.info("Restart PostgreSQL service to apply configuration..")
        os.system(f"sshpass -p {p.POSTGRES_SERVER_PASSWSD} ssh {self.remote_ip} 'echo {p.POSTGRES_SERVER_PASSWSD} | sudo -S systemctl restart postgresql'")
        logging.info("Restart PostgreSQL service finished..")