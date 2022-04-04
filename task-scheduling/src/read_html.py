import os


def read(html_file: str):
    ping_cai_home = os.getenv('PING_CAI_HOME')
    relative_path = ("%s/rail-transit/sicau-rail_transit-1.0-SNAPSHOT-bin/scheduler/html" % ping_cai_home)
    file = open("%s/%s" % (relative_path, html_file), "r+", encoding="UTF-8")
    content: str = file.read()
    file.close()
    return content


if __name__ == '__main__':
    print(read("distribution_alarm_test.html"))
