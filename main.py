# project: p4
# submitter: ikapoor4
# partner: none
# hours: 12

#import statements 
import pandas as pd
import zipfile
from flask import Flask, request,Response,jsonify
import subprocess
import re
import time 
from collections import Counter
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import geopandas as gpd
from io import BytesIO
from flask import *
from shapely.geometry import Point
import bisect
import netaddr

app = Flask(__name__)

#global variables 
visit_count = 0 
current_version = None 
click_count = {'A': 0, 'B': 0}

@app.route('/')
def home():
    
    global visit_count
    global current_version
    visit_count += 1
    if visit_count <= 10:
        current_version = 'A' if visit_count % 2 != 0 else 'B'
    else:
        current_version = max(click_count, key=click_count.get)

    with open(f"index_{current_version}.html") as f:
        html = f.read()
    return html

@app.route('/browse.html')
def browse():
    
    output = subprocess.check_output("unzip -p server_log.zip rows.csv | head -n 501", shell=True)
    output = output.decode('utf-8')
    output = re.split(",|\r\n", output)
    final_output = [output[i:i+15] for i in range(0, len(output), 15)]
    df = pd.DataFrame(final_output[1:501], columns=['ip', 'date', 'time', 'zone', 'cik', 'accession', 'extention', 'code', 'size', 'idx', 'norefer', 'noagent', 'find', 'crawler', 'browser'])
    df.index = df.index + 1
    return "<h1><b>Browse first 500 rows of rows.csv</b></h1>" + df.to_html()

last_access_times = {}
visitors=[]

@app.route('/browse.json')
def browse_json():
    
    ip = request.remote_addr
    current_time = time.time()
    if ip in last_access_times and current_time - last_access_times[ip] < 60:
        retry_after = 60 - (current_time - last_access_times[ip])
        # Using flask.Response to send the status code and headers
        response = Response("Exceeded Rate Limit", status=429)
        response.headers['Retry-After'] = str(int(retry_after))
        return response
    else:
        # Updating the last access time
        last_access_times[ip] = current_time
        # Add the IP to visitors list if not already added
        if ip not in visitors:
            visitors.append(ip)

        # Processing the request
        output = subprocess.check_output("unzip -p server_log.zip rows.csv | head -n 501", shell=True)
        output = output.decode('utf-8')
        output = re.split(",|\r\n", output)
        final_output = [output[i:i+15] for i in range(0, len(output), 15)]
        df = pd.DataFrame(final_output[1:501], columns=['ip', 'date', 'time', 'zone', 'cik', 'accession', 'extension', 'code', 'size', 'idx', 'norefer', 'noagent', 'find', 'crawler', 'browser'])
        json_data = df.to_dict(orient='records')
        response = jsonify(json_data)
        response.headers['X-RateLimit-Limit'] = '1'
        response.headers['X-RateLimit-Remaining'] = '0'
        response.headers['X-RateLimit-Reset'] = str(int(current_time + 60))
        
        return response

@app.route('/visitors.json')
def visitors_json():
    
    return jsonify(visitors)

@app.route('/donate.html')
def donate():
    
    v1 = request.args.get('from', '')
    if v1:
        click_count[v1] = click_count.get(v1, 0) + 1
        return "<h1><b>Donations</b></h1>" + "Please donate to this cause, since this cause is powered by your support. It is essential for your support so that it    can reach new heights, and support the community."
    else:
        return "<h1><b>Donations</b></h1>" + "Please donate to this cause, it will be of great kindness and support to the community if done so."

@app.route('/analysis.html')
def analysis():
    
    filings = read()
    log_server_csv = pd.read_csv('server_log.zip', compression = 'zip')
    top_ips = log_server_csv.groupby('ip').size().nlargest(10)
    top_ips_dict = top_ips.to_dict()
    sic_distribution = {}
    for filing in filings.values():
        if filing.sic != None:
            sic_distribution[filing.sic]=sic_distribution.get(filing.sic, 0)+1
    top_sic_distribution = dict(sorted({k: v for k, v in sic_distribution.items() if k is not None}.items(), key=lambda item: item[1], reverse=True)[:10])

    address_count = Counter()
    for filing in filings.values():
        for address in filing.addresses:
            address_count[address] += 1
    common_addresses = {address: count for address, count in address_count.items() if count >= 100}
    
    final_html = "<h1>Analysis of EDGAR Web Logs</h1>\n"
    final_html= final_html + "<p>Q1: how many filings have been accessed by the top ten IPs?</p>\n"
    final_html= final_html +"<p>{}</p>\n".format(top_ips_dict)
    final_html= final_html +"<p>Q2: what is the distribution of SIC codes for the filings in docs.zip?</p>\n"
    final_html = final_html +"<p>{}</p>\n".format(top_sic_distribution)
    final_html= final_html +"<p>Q3: what are the most commonly seen street addresses?</p>\n"
    final_html =final_html + "<p>{}</p>\n".format(common_addresses)
    final_html = final_html +"<h4>Dashboard: geographic plotting of postal code</h4> <img src='dashboard.svg'>"
    return final_html

def extract_postal_codes(address_dict):
    postal_codes = {}
    postal_code_regex = re.compile(r'\b\d{5}(?:-\d{4})?\b')
    for address, count in address_dict.items():
        search = postal_code_regex.search(address)
        if search:
            postal_code = search.group()
            postal_codes[address] = postal_code
    return postal_codes

@app.route('/dashboard.svg')
def map_plot():

    locations_gdf = gpd.read_file('locations.geojson')
    background_map = gpd.read_file('shapes/cb_2018_us_state_20m.shp')
    locations_gdf = locations_gdf.cx[-95:-60, 25:50]
    locations_gdf['postal_code'] = '50000'
    fig, ax = plt.subplots(figsize=(10, 8))
    background_map.plot(ax=ax, color='lightgray')
    scatter = locations_gdf.plot(ax=ax, column='postal_code', cmap='RdBu', legend=True, marker='o', markersize=50)

    ax.set_aspect('equal')
    ax.set_axis_off()
    ax.set_xlim([-95, -60])
    ax.set_ylim([25, 50])
    ax.set_aspect(1.5)
    dashboard = BytesIO()
    plt.savefig(dashboard, format='svg')
    plt.close(fig)
    dashboard.seek(0)
    response = make_response(dashboard.getvalue())
    response.headers['Content-Type'] = 'image/svg+xml'
    return response

class Filing:
    def __init__(self, html):
        self.dates = self.dates(html)
        self.sic = self.sic(html)
        self.addresses = self.addresses(html)

    def dates(self, html):
        pattern = r"\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\b"
        matches = re.findall(pattern, html)
        
        final_matches = [date for date in matches if 1900 <= int(date[:4]) <= 2100 and 1 <= int(date[5:7]) <= 12]
        return final_matches

    def sic(self, html):
        pattern = r'SIC[^0-9]*?(\d{4})\b'
        match = re.search(pattern, html)
        if match :
                return int(match.group(1))
        else:
            return None
            
        
        

    def addresses(self, html):
        addresses_list = []
        pattern = r'<div class="mailer">([\s\S]+?)</div>'
        contents = re.findall(pattern, html)
        
        
        for content in contents:
            lines = []
            line_pattern = r'<span class="mailerAddress">([\s\S]+?)</span>'
            for line in re.findall(line_pattern, content):
                filtered_line = line.strip().replace('\n', ' ').replace('\r', '').replace('  ', ' ')
                
                if filtered_line:
                    lines.append(filtered_line)
            if lines:
                formatted_address = ' '.join(lines)
                addresses_list.append(formatted_address)
        return addresses_list

    def state(self):
        for address in self.addresses:
            pattern = re.compile(r'\b[A-Z]{2} \d{5}\b')
            match = pattern.search(address)
            if match:
                return match.group()[:2]
        return None

def read():
    filings = {}
    with zipfile.ZipFile("docs.zip") as archive:
        for file_info in archive.infolist():
            with archive.open(file_info) as file:
                html = file.read().decode('utf-8')
                filing = Filing(html)
                filings[file_info.filename] = filing
                
    return filings       






    
    

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, threaded=False) # don't change this line!

# NOTE: app.run never returns (it runs for ever, unless you kill the process)
# Thus, don't define any functions after the app.run call, because it will
# never get that far.
