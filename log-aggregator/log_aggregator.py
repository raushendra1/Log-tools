"""Script to collect all the logs and configuration files."""

import shutil
import os
import sys

"""Error if input file does not exists."""
def explain_error():
    print("Input.txt could not be located, Exiting!!! Please contact unravel support.")
if not os.path.isfile('input.txt'):
    sys.exit(explain_error()) 
 
""" Parsing input file to get application type and Unravel home directory. """
with open('input.txt','r') as apps:
    attribute=apps.readlines()
    application_type=attribute[3].split('=')[1].strip('\n')
    unravel_home=attribute[6].split('=')[1].strip('\n')
    install_type=attribute[9].split('=')[1].strip('\n')
    output_path ="{}{}".format(unravel_home, "/dump")
    if os.path.exists("{}/dump".format(unravel_home)):
        os.system("rm -rf {}/dump".format(unravel_home))
    os.system("mkdir {}/dump".format(unravel_home))
    print "Aggregated log path={}".format(output_path)
    print "Application type={}".format(application_type)

if install_type == 'new_installer':

    """ Hive log files. """
    hive_logs=['log_receiver.log','event_worker_*','hive_worker.log']

    """ Hive configuration files. """
    hive_conf=['/etc/hive/conf/hive-site.xml','/etc/hive/conf/hive-env.sh']

    """ Impala log files. """
    impala_logs=['unravel_sensor_*']

    """ Spark log files. """
    spark_logs=['spark_worker_*','yarn_jc_sensor.log','event_worker_*','log_receiver.log']

    """ Spark configuration files. """
    spark_conf=["{}/unravel/data/conf/unravel.properties".format(unravel_home)]

    """ TEZ log files. """
    tez_logs=['yarn_jc_sensor.log','event_worker_*','log_receiver.log','hitdoc_loader.*']

    """ TEZ configuration files. """
    tez_conf=["{}/unravel/data/conf/unravel.properties".format(unravel_home)]

else:
    
    """ Hive log files. """
    hive_logs=['unravel_lr.log','unravel_ew_*','unravel_hhwe.log']

    """ Hive configuration files. """
    hive_conf=['/etc/hive/conf/hive-site.xml','/etc/hive/conf/hive-env.sh']

    """ Impala log files. """
    impala_logs=['unravel_us_*']

    """ Spark log files. """
    spark_logs=['unravel_sw_*','unravel_jcs2.log','unravel_ew_*','unravel_lr.log']

    """ Spark configuration files. """
    spark_conf=["{}/unravel/etc/unravel.properties".format(unravel_home)]

    """ TEZ log files. """
    tez_logs=['unravel_jcs2.log','unravel_ew_*','unravel_lr.log','unravel_hl.*']
    
    """ TEZ configuration files. """
    tez_conf=["{}/unravel/etc/unravel.properties".format(unravel_home)]

""" Copying logs and config files. """
""" Hive application """
if application_type == 'hive':
    for files in hive_logs:
        log_path = "{}{}".format(unravel_home, "/unravel/logs/{}".format(files))
        os.system("cp -r {} {}".format(log_path,output_path)) 
    for conf in hive_conf:
        shutil.copy(conf , output_path)

elif application_type == 'impala':
    for files in impala_logs:
        log_path = "{}{}".format(unravel_home, "/unravel/logs/{}".format(files))
        os.system("cp -r {} {}".format(log_path,output_path))

elif application_type == 'spark':
    for files in spark_logs:
        log_path = "{}{}".format(unravel_home, "/unravel/logs/{}".format(files))
        os.system("cp -r {} {}".format(log_path,output_path))    
    for conf in spark_conf:
        shutil.copy(conf , output_path) 

elif application_type == 'tez':
    for files in tez_logs:
        log_path = "{}{}".format(unravel_home, "/unravel/logs/{}".format(files))
        os.system("cp -r {} {}".format(log_path,output_path))
    for conf in tez_conf:
        shutil.copy(conf , output_path) 

""" Check and delete existing tar file. """
if os.path.exists("{}/dump.tar.gz".format(unravel_home)):
        os.system("rm -rf {}/dump.tar.gz".format(unravel_home))

""" Creating tar of dump directory """
os.system("tar -czvf {}/dump.tar.gz {}/dump".format(unravel_home,unravel_home))
print ("Tar file: {}/dump.tar.gz".format(unravel_home))


