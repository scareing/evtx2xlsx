import pandas as pd
import sys
from Evtx.Evtx import Evtx
from Evtx.Views import evtx_file_xml_view
import xml.etree.ElementTree as ET

def parse_evtx_to_excel(evtx_path, output_excel_path):
    records = []
    event_count = 0
    matched_events = 0

    print(f"Opening EVTX file: {evtx_path}")

    with Evtx(evtx_path) as log:
        for xml, _ in evtx_file_xml_view(log):
            event_count += 1
            # Debug output for progress
            if event_count % 100 == 0:
                print(f"Processed {event_count} events...")

            root = ET.fromstring(xml)
            event_id_element = root.find('.//{http://schemas.microsoft.com/win/2004/08/events/event}EventID')
            event_id = event_id_element.text if event_id_element is not None else None

            # Filter only for login events
            if event_id not in ["4624", "4625"]:
                continue

            matched_events += 1

            timestamp_element = root.find('.//{http://schemas.microsoft.com/win/2004/08/events/event}TimeCreated')
            timestamp = timestamp_element.attrib['SystemTime'] if timestamp_element is not None else None

            # Extract data elements
            user_name = None
            domain = None
            ip_address = None
            logon_type = None

            for data in root.findall('.//{http://schemas.microsoft.com/win/2004/08/events/event}Data'):
                if data.attrib.get('Name') == 'TargetUserName':
                    user_name = data.text
                elif data.attrib.get('Name') == 'TargetDomainName':
                    domain = data.text
                elif data.attrib.get('Name') == 'IpAddress':
                    ip_address = data.text
                elif data.attrib.get('Name') == 'LogonType':
                    logon_type = data.text

            records.append({
                'EventID': event_id,
                'Timestamp': timestamp,
                'UserName': user_name,
                'Domain': domain,
                'IpAddress': ip_address,
                'LogonType': logon_type
            })

    print(f"Finished processing {event_count} events.")
    print(f"Matched {matched_events} login events. Saving to Excel...")

    df = pd.DataFrame(records)
    if not df.empty:
        df.to_excel(output_excel_path, index=False)
        print(f"Data saved to {output_excel_path}")
    else:
        print("No matching login events found.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 evtx2xlsx.py input.evtx output.xlsx")
        sys.exit(1)

    evtx_path = sys.argv[1]
    output_excel_path = sys.argv[2]

    parse_evtx_to_excel(evtx_path, output_excel_path)