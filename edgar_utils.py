import pandas as pd
import re
import bisect
import netaddr

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

    
def lookup_region(address_string):
    ip_address = address_string
    ip_address = re.sub(r"[a-z]", r"0", ip_address)
    ips = pd.read_csv("ip2location.csv")
    index = bisect.bisect_right(ips["low"], int(netaddr.IPAddress(ip_address))) - 1
    return ips.iloc[index]["region"]