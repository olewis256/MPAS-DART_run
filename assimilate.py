import os, sys, shutil
import datetime as dt
import yaml
import f90nml
import lxml.etree as etree
import time
import subprocess
import shutil
import pandas as pd
import re

def slink(src, dst):

    try:  
        os.remove(dst)
    except OSError:
        pass

    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.symlink(src, dst)

def pbs_script(inputs, path):
    PBS_TEMPLATE = """#!/bin/bash
#PBS -N {job_name}
#PBS -j oe
#PBS -o {log_path}
#PBS -A {proj_number}
#PBS -q {queue}
#PBS -l walltime={walltime}
#PBS -l select={nodes}:ncpus={ncpus}:mpiprocs={mpiprocs}:mem={mem}GB
module load ncarenv

cd {work_dir}
./perfect_model_obs
"""
    with open(path, 'w') as f:
        f.write(PBS_TEMPLATE.format(**inputs))

def run_pmo(out_file, date):

    time_str = date.strftime('%Y%m%d%H')
    job_name_str = 'pmo_obs_' + time_str

    pbs_inputs = {
        "job_name": job_name_str,
        "log_path": output_file,
        "proj_number": 'NMMM0063',
        "queue": 'develop',
        "walltime": '00:30:00',
        "nodes": '1',
        "ncpus": '16',
        "mpiprocs": '16',
        "mem": '64',
        "work_dir": run_dir
    }

    pbs_script(pbs_inputs, pbs_path)

    comd = 'qsub pmo.pbs > ' + out_file
    os.system(comd)

    time.sleep(15)

    while True:

        output = subprocess.check_output("qstat -u $USER", shell=True)

        output = str(output)
        match = re.search(r'(\d+)\.desche\*', output)
        if match:
            job_id = match.group(1)
            print(f"Job ID: {job_id}")
        else:
            print("No job ID found in the output.")
            break
        time.sleep(10)

if __name__ == '__main__':

    with open('params.yaml', 'r') as file:
        config = yaml.safe_load(file)

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    interval = sys.argv[3]

    start_date = dt.datetime.strptime(start_date, '%Y%m%d%H')
    end_date = dt.datetime.strptime(end_date, '%Y%m%d%H')

    year_init = start_date.year
    month_init = start_date.month
    day_init = start_date.day
    hour_init = start_date.hour

    date_list = pd.date_range(start=start_date,
                              end=end_date,
                              freq=f'{interval}h').to_pydatetime()
    
    print(date_list)
    
    for date in date_list:

        curr_date = date
        print('Processing date:', curr_date)
        year_curr = curr_date.year
        month_curr = curr_date.month
        day_curr = curr_date.day
        hour_curr = curr_date.hour

        curr_date_str = curr_date.strftime('%Y%m%d%H')
        
        proj_dir = config['proj_dir']
        run_dir = config['run_dir']
        os.makedirs(run_dir, exist_ok=True)
        filter_src= config['DART_dir'] + '/models/mpas_atm/work/filter'
        time_src = config['DART_dir'] + '/models/mpas_atm/work/advance_time'

        rtcoef_file = config['rtcoef_file']
        sccldcoef_file = config['sccldcoef_file']

        slink(filter_src, run_dir+'/filter')
        slink(time_src, run_dir+'/advance_time')

        slink(rtcoef_file, run_dir+'/rtcoef_dummy_5_dummyir.dat')
        slink(sccldcoef_file, run_dir+'/sccldcoef_dummy_5_dummyir.dat')

        obs_kind = config['obs_kind']
        obs_dir = config['obs_dir']
        obs_seq_in = obs_dir + 'obs_seq.out.' + curr_date_str

        member = '/member00'

        inout_files = config['inout_state'] 
        init_files = config['init_state']
        mpas_vars = config['mpas_vars']

        x = etree.parse(config['MPAS_stream'])
        root = x.getroot()

        for stream in root.findall('stream'):
            if stream.get('name') == 'da_restart':
                stream.set('output_interval', '6:00:00')
                # etree.SubElement(stream, 'var', {'name': 'new_variable'})
                # for var in stream.findall('var'):
                #     if var.get('name') == 'cldfrac':
                #         stream.remove(var)
                        
        x.write('streams.atmosphere.edited', pretty_print=True)

        mpas_file_time = f'{year_curr}-{month_curr:02d}-{day_curr:02d}_{hour_curr:02d}.00.00'

        input_state = f'{inout_files}{curr_date_str}{member}/mpasout.{mpas_file_time}.nc'
        output_state = f'{inout_files}{curr_date_str}{member}/perfect_{curr_date_str}.nc'
        init_state = f'{init_files}{member}/x1.10242.init.nc'

        print('Input state: ', input_state)

        nml_file = f90nml.read(proj_dir + '/templates/input.nml')
        nml_file['model_nml']['init_template_filename'] = init_state
        nml_file['obs_kind_nml']['assimilate_these_obs_types'] = obs_kind
        nml_file['mpas_vars_nml']['mpas_state_variables'] = mpas_vars

        nml_file['filter_nml']['ens_size'] = config['filter']['ens_size']
        nml_file['filter_nml']['num_output_obs_members'] = config['filter']['num_output_obs_members']
        nml_file['filter_nml']['assimilation_period_days'] = config['filter']['assimilation_period_days']
        nml_file['filter_nml']['cutoff'] = config['filter']['cutoff']
        nml_file['filter_nml']['vert_normalization_height'] = config['filter']['vert_normalization_height']
        nml_file['filter_nml']['distribute_mean'] = config['filter']['distribute_mean']
        nml_file['filter_nml']['convert_all_obs_verticals_first'] = config['filter']['convert_all_obs_verticals_first']
        nml_file['filter_nml']['write_binary_obs_sequence'] = config['filter']['write_binary_obs_sequence']
        nml_file['filter_nml']['tasks_per_node'] = config['filter']['tasks_per_node']

        if config['filter']['adaptive_inf'] == 'true':
            inf_temp = config['filter']['input_priorinf']
            infl_out_mean_file = f'{inout_files}{prev_date_str}{member}{inf_temp}_mean.nc'
            infl_out_sd_file = f'{inout_files}{prev_date_str}{member}{inf_temp}_mean.nc'
            if os.path.exists(infl_out_mean_file):
                print("Found file: ", infl_out_mean_file, ", copying...")
                slink(infl_out_mean_file, run_dir+infl_out_mean_file)
                slink(infl_out_sd_file, run_dir+infl_out_sd_file)
                icyc = curr_date_str
            else:
                cmd = [f'find ${inout_files}/*/ -name "${inf_temp}_mean.nc" -print -quit | wc -l || echo 0']
                check = subprocess.run(cmd, capture_output=True, text=True)
                if len(check) > 0:
                    print("Check")
                else:
                    print("check again")
                    icyc = 0

        if icyc == 0:
            nml_file['filter_nml']['inf_initial_from_restart']    = '.false.,          .false.,'
            nml_file['filter_nml']['inf_sd_initial_from_restart'] = '.false.,          .false.,'
        else:
            nml_file['filter_nml']['inf_initial_from_restart']    = '.true.,          .true.,'
            nml_file['filter_nml']['inf_sd_initial_from_restart'] = '.true.,          .true.,'
]
            
        try:
            nml_file.write(run_dir+'input.nml', force=True)
        except Exception as e:
            print("Error writing input.nml:", e)

        output_file = 'output.filter_'+curr_date_str+'.log'

        pbs_path = 'pmo.pbs'

        print("Running filter for date:", curr_date_str)
        run_pmo(output_file, curr_date)

        pmo_txt = open(output_file).readlines()

        log_dir = '/glade/derecho/scratch/olewis/python_scripts/logs/'

        if ' Finished ... at YYYY MM DD HH MM SS = \n' not in pmo_txt:
            print('Process did not finish correctly')
            status = -1
            break
        else:
            shutil.move(output_file,
                        log_dir+output_file)

            os.system('rm '+output_file)
            status = 0

            os.rename(run_dir+'obs_seq.final', run_dir+'obs_seq.final.'+curr_date.strftime('%Y%m%d%H'))
            
            shutil.move(run_dir+'obs_seq.final.'+curr_date.strftime('%Y%m%d%H'),
                        obs_dir+'obs_seq.final.'+curr_date.strftime('%Y%m%d%H'))

    

    