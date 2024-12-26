#!/awips2/python/bin/python

# original author(s):   henry pai (nwrfc)
# contact info:         henry <dot> pai <at> noaa <dot> gov
# last edit by:         hp
# last edit time:       Nov 2024
# last edit comment:    added some comments/documentation inline

# nrcs rest api: https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html
# element codes: https://www.nrcs.usda.gov/wps/portal/wcc/home/dataAccessHelp/webService/webServiceReference/!ut/p/z1/jZBNb4JAEEB_iweOMgNscfW2blKlGA0VIs7FgKErCbIGsaT_XmO9mOiGuc3kvfkCghSozn5LlbWlrrPqlm_J33EumbNiuJgtYw8j_1smn_PQRQdhcwcCl4s5X2M4k4yhGMtRvFiPMJwiUB8f34To6RsAMrffAD2PQB64GMkoZKsP9FA6D8B0ohGI_Qdg2OILSFU6_3-4qHOPK6Cm-CmaorEvza18aNvTeWKhhV3X2UprVRX2Xh8tfKUc9LmF9JmE0zFJUiyDIeV_nRgMrpB2Ka0!/#elementCodes
# scraper timing: ~2.5 - ~3.5 min for scraping alone on windows pc, comcast isp.  this was for > 300 stations, looking 3 hours back
# good to change at other offices:
# - request headers
# - shef headers (in make_shef function)
# - global vars at top of script

# will be called by shell script, can be forked to the web:
# get_snotel_shef.py --locid all --params PREC,TOBS,WTEQ,SNWD --duration HOURLY --back 3  <- no arguments will call this by default, 3 min runtime
# get_snotel_shef.py --locid all --params WTEQ --duration DAILY --back 10 <- good for bringing in nrcs QC. OR,WA weekly, MT daily (from Amy Burke)
# get_snotel_shef.py --locid CLJW1 --params PREC,TOBS,WTEQ,SNWD --duration HOURLY --back 7 <- good for testing 

# TODO/FUTURE:
# [ ] incorporate other networks? scan, snolites?

import os
import urllib
import requests
import argparse
import pandas as pd
import pathlib
import logging
from datetime import datetime, timezone
import shutil
import yaml
import pdb

os.umask(0o002)

# ===== global var (not path related)
out_fmt = "shef"  # csv or shef
#max_call_ids = 3
max_call_ids = 95 # 95 stations = 1865 characters for data call, 1805 for metadata call, 2048 max call
code_dict = {'PREC' : 'PC', 'TOBS' : 'TA', 'WTEQ' : 'SW', 'SNWD' : 'SD'}
type_source = 'RB'
product_id = 'snotelWeb'

# ===== directories & filenames
if os.name == 'nt':
    work_dir = pathlib.Path(__file__).parent # IDE independent
    meta_dir = os.path.join(work_dir, "meta")
    out_dir = os.path.join(work_dir, "incoming")
    log_dir = os.path.join(work_dir, "logs")
else:
    work_dir = pathlib.Path("/data/ldad/snotel/")
    meta_dir = work_dir
    out_dir = pathlib.Path("/data/Incoming/")
    log_dir = pathlib.Path("/data/ldad/logs/")

meta_fn = "SNOTEL_metadata_2024.csv"
log_fn = "snotel_scrape.log"
out_fn_pre = "snotel_scraped_"
new_fn_pre = "new_snotel_"
last_fn_pre = "last_snotel_"
yaml_fn = 'config.yaml'

with open(os.path.join(meta_dir, yaml_fn)) as f:
    yaml_data = yaml.full_load(f)
    request_headers = {'User-Agent' : yaml_data['user_agent']}

# ===== url info, different between swe and precip
data_url = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?"
meta_url = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations?"

# ===== initial set up for requests and logging
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(message)s',
                    filename=os.path.join(log_dir, log_fn),
                    filemode='w',
                    #level=logging.DEBUG,
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# ===== functions
def parse_args():
    """
    Sets default arguments, just for hour look back
    Default look back is three hours
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--locid',
        #default='CLJW1', # testing
        default='all',
        help="Location id (all/<LID of single station>)"
        )
    parser.add_argument(
        '--params',
        default='PREC,TOBS,WTEQ,SNWD',
        #default='WTEQ', # testing
        help="Comma separated string for NRCS parameters"
        )
    parser.add_argument(
        '--duration',
        #default='DAILY',
        default='HOURLY',
        help="Set interval (HOURLY/DAILY)"
        )
    parser.add_argument(
        '--back',
        default=3, # 3 hours or days
        help="Interval back in units of duration <int>"
        )

    return parser.parse_args()

def get_data(triplet_str, params_str, duration, back):
    """
    getting the data via rest calls.  nesting was tricky to make a minimalist call, but compact enough for me
    """
    request_params = {'stationTriplets': ','.join(triplet_str),
                      'elements': params_str,
                      'duration': duration,
                      'beginDate': '-' + str(back)
                      }
    nrcs_url = data_url + urllib.parse.urlencode(request_params)
    #pdb.set_trace()
    response = requests.get(nrcs_url, headers=request_headers)
    json_strings = response.json()

    all_gages_li = []
    for json_str in json_strings:
        # some testing to get meta column
        gage_df = pd.json_normalize(json_str['data'], record_path='values', meta=[['stationElement', 'elementCode']])
        gage_df['stationTriplet'] = json_str['stationTriplet']
        all_gages_li.append(gage_df)

    gages_df = pd.concat(all_gages_li)
    gages_df['PE'] = [code_dict.get(nrcs_code) for nrcs_code in gages_df['stationElement.elementCode']] 

    return(gages_df)

def get_meta(triplet_str):
    """
    grabbing metadata for shef Id and time zone for gage - will convert back to GMT/UTC
    """
    request_params = {'stationTriplets': ','.join(triplet_str)}
    nrcs_url = meta_url + urllib.parse.urlencode(request_params)
    #pdb.set_trace()
    response = requests.get(nrcs_url, headers=request_headers)
    json_strings = response.json()
    meta_df = pd.DataFrame(json_strings)[['stationTriplet', 'dataTimeZone', 'shefId']]
    return(meta_df)

def write_new_lines(last_fullfn, new_fullfn, out_fullfn, out_fmt):
    """
    compare last file download and current file download, write only new lines
    """
    # skipping header rows
    if out_fmt == 'csv':
        start_row = 1
    elif out_fmt == 'shef':
        start_row = 2
    
    # save differences
    with open(new_fullfn, 'r') as newfile:
        new_lines = newfile.readlines()[start_row:]
        with open(last_fullfn, 'r') as lastfile:
            last_lines = lastfile.readlines()[start_row:]
            
            set_last = set(last_lines)
            diff = [x for x in new_lines if x not in set_last]

    # write new data to file or delete header file
    if len(diff) > 1:
        with open(out_fullfn, 'a') as file_out:
            for line in diff:
                file_out.write(line)
    else:
        logging.info("no new lines of data observed, deleting " + out_fullfn)
        os.remove(out_fullfn)

def write_header(utc_now, out_fullfn, out_fmt, header=None):
    """
    adds two line header for shef:
    TTAA00 KPTR <ddhhmm>
    snotelWeb

    just csv row header for csv
    """
    f = open(out_fullfn, 'w')
    
    if out_fmt == 'shef':
        f.write("TTAA00 KPTR " + utc_now.strftime("%d%H%M") + "\n")
        f.write(product_id + "\n")  # product_id
    elif out_fmt == 'csv':
        header_str = ','.join(header) + '\n'
        f.write(header_str)
        
    f.flush()
    f.close()

def remove_dup_lines(out_fullfn):
    """
    removes duplicate lines

    https://stackoverflow.com/questions/1215208/how-might-i-remove-duplicate-lines-from-a-file
    """
    shutil.copyfile(out_fullfn, out_fullfn + ".tmp")

    lines_seen = set() # holds lines already seen
    outfile = open(out_fullfn, "w")
    for line in open(out_fullfn + ".tmp", "r"):
        if line not in lines_seen: # not a duplicate
            outfile.write(line)
            lines_seen.add(line)
    outfile.close()

    os.remove(out_fullfn + ".tmp")

def main():
    arg_vals = parse_args()
    snotel_meta_df = pd.read_csv(os.path.join(meta_dir, meta_fn))

    logging.info('scraping started')

    # getting data and putting in data.frame - all stations in metadata list or single station
    if arg_vals.locid == 'all':
        data_li = []
        meta_li = []
        for i in range(0, len(snotel_meta_df), max_call_ids):
            subset_df = snotel_meta_df.iloc[i:(i + max_call_ids)]
            triplet_str = subset_df['StationId'].astype(str) + ':' + subset_df['StateCode'] + ':SNTL'
            call_data_df = get_data(triplet_str, arg_vals.params, arg_vals.duration, arg_vals.back)
            call_meta_df = get_meta(triplet_str)
            data_li.append(call_data_df)
            meta_li.append(call_meta_df)
        all_data_df = pd.concat(data_li)
        all_meta_df = pd.concat(meta_li)
    else:
        try:
            row_info = snotel_meta_df[snotel_meta_df['ShefId'] == arg_vals.locid]
            triplet_str = row_info['StationId'].astype(str) + ':' + row_info['StateCode'] + ':SNTL'
            all_data_df = get_data(triplet_str, arg_vals.params, arg_vals.duration, arg_vals.back)
            all_meta_df = get_meta(triplet_str)
        except:
            logging.info('improper character length for LID')

    logging.info('scraping completed')

    # beginning output and filtering out duplicate rows from hitting IHFS
    utc_now = datetime.now(timezone.utc) # changed utcnow() call as it will be deprecated by 3.12

    combined_df = all_data_df.merge(all_meta_df)
    # all data downloaded in local time, converting data back to UTC/GMT
    combined_df['utcTime'] = pd.to_datetime(combined_df['date']) - pd.to_timedelta(combined_df['dataTimeZone'], 'h')

    final_df = combined_df[['shefId', 'utcTime', 'PE', 'value']]

    fn_time_str = utc_now.strftime('%Y%m%d_%H%M%S')

    # 3 files needed to minimize database writing
    out_fn = out_fn_pre + arg_vals.duration + "." + fn_time_str + "." + out_fmt

    new_fn = new_fn_pre + arg_vals.duration + "." + out_fmt
    last_fn = last_fn_pre + arg_vals.duration + "." + out_fmt

    new_fullfn = os.path.join(log_dir, new_fn)
    last_fullfn = os.path.join(log_dir, last_fn)

    final_out_fullfn = os.path.join(out_dir, out_fn)

    if arg_vals.duration == 'HOURLY':
        shef_dur = 'I'
    else:
        shef_dur = 'D'

    if out_fmt =='csv':
        csv_df = final_df.copy()
        csv_df['duration'] = arg_vals.duration
        csv_df.to_csv(new_fullfn, index=False)
        csv_headers = csv_df.columns
    else:
        write_header(utc_now, new_fullfn, out_fmt)
        
        # ZZ is extremum (none) and probability (none)
        # DUE - E part means egnlish units
        out_lines = (".AR " + final_df.shefId + " " 
                     + final_df.utcTime.dt.strftime('%Y%m%d') + " Z DH" + final_df.utcTime.dt.strftime("%H%M")
                     + "/DUE /" + final_df.PE + shef_dur + type_source + "ZZ " + final_df.value.astype(str)) 
        with open(new_fullfn, 'a') as f:
            f.write("\n".join(out_lines))

    if os.path.isfile(last_fullfn):
        if out_fmt == "csv":
            write_header(utc_now, final_out_fullfn, out_fmt, header=csv_headers)
        if out_fmt == "shef":
            write_header(utc_now, final_out_fullfn, out_fmt)
        write_new_lines(last_fullfn, new_fullfn, final_out_fullfn, out_fmt)
    else:
        shutil.copyfile(new_fullfn, final_out_fullfn)

    # removes duplicate lines within single file
    if os.path.isfile(final_out_fullfn):
        remove_dup_lines(final_out_fullfn)

    shutil.copyfile(new_fullfn, last_fullfn)
    logging.info('nrcs scraping complete with output to: ' + final_out_fullfn)
    logging.info('equivalent final output found in last file: ' + last_fullfn)

if __name__ == '__main__':
    main()

