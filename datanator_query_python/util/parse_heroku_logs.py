"""Parsing Heroku app datanator-rest-api
"""
import re


class ParseLogs:

    def __init__(self, file_location=None, verbose=True):
        """Init

        Args:
            file_location(:obj:`str`): location of log file.
            verbose(:obj:`bool`): print verbose message.
        """
        self.file_location = file_location
        self.verbose = verbose

    def parse_regex(self, s, regex):
        """Find in line matches for regulation expressions.

        Args:
            s(:obj:`str`): String in which to find matching regular expression.
            regex(:obj:re`): regular expression.

        Return:
            (:obj:`list`): Matches found. 
        """
        return re.search(regex, s)

    def parse_router(self, lines=0):
        """Parse router lines to get API performance values.

        Args:
            lines(:obj:`int`, optional): Number of lines to parse. If 0, parse all lines.

        Return:
            (:obj:`Obj`)
        """
        result = {}
        with open(self.file_location, 'r') as _file:
            for i, line in enumerate(_file):
                if lines != 0 and i == lines:
                    break
                if line.endswith('\n'):
                    line = line[0:-1]
                end_point_match = re.search(r'(\/.*\w+\/)', line)
                performance_match = re.search(r'service=(\d*)ms', line)
                if end_point_match and performance_match:
                    end_point = end_point_match.group(1)
                    performance = int(performance_match.group(1))
                    if result.get(end_point) is None:
                        result[end_point] = [performance]
                    else:
                        result[end_point] += [performance]
                else:
                    continue
        return result


import numpy as np
import matplotlib.pyplot as plt

def main():
    file_location = "./docs/20200430-logs-1500.txt"
    manager = ParseLogs(file_location=file_location)
    result = manager.parse_router()
    x_label = []
    std = []
    mean = []
    for _key, val in result.items():
        x_label.append(_key)
        array = (np.array(val))
        std.append(np.std(array))
        mean.append(np.mean(array))
    x_pos = np.arange(len(x_label))
    fig, ax = plt.subplots()
    ax.barh(x_pos, mean, xerr=std, align='center', ecolor='black', capsize=5)
    ax.set_xlabel('Speed (ms)')
    ax.set_yticks(x_pos)
    ax.set_yticklabels(x_label)
    ax.invert_yaxis()
    ax.set_title('Performance snapshot of REST API endpoints')
    ax.xaxis.grid(True)
    # plt.tight_layout()
    plt.savefig('api_performance.png')
    plt.show()    

if __name__ == '__main__':
    main()